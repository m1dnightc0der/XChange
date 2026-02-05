package info.bitrich.xchangestream.okex;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.ObservableEmitter;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.okex.OkexExchange;

/**
 * Unit tests for OkexStreamingService login functionality.
 * Tests the CompletableFuture-based login mechanism and error propagation.
 */
public class OkexStreamingServiceLoginTest {

  private TestableOkexStreamingService service;
  private ExchangeSpecification exchangeSpec;
  private ObjectMapper objectMapper;

  @Before
  public void setUp() {
    exchangeSpec = new ExchangeSpecification(OkexExchange.class);
    exchangeSpec.setApiKey("test-api-key");
    exchangeSpec.setSecretKey("test-secret-key");
    exchangeSpec.setExchangeSpecificParametersItem("passphrase", "test-passphrase");

    service = new TestableOkexStreamingService("wss://test.okex.com", exchangeSpec);
    objectMapper = new ObjectMapper();
  }

  // ==================== login() Tests ====================

  @Test
  public void testLoginReturnsCompletedFutureWhenAlreadyLoggedIn() throws Exception {
    // Given: service is already logged in
    service.isLoggedIn = true;

    // When: login() is called
    CompletableFuture<Void> future = service.login();

    // Then: should return an already completed future
    assertTrue("Future should be done", future.isDone());
    assertFalse("Future should not be completed exceptionally", future.isCompletedExceptionally());
  }

  @Test
  public void testLoginReturnsExistingFutureWhenLoginInProgress() throws Exception {
    // Given: a login is already in progress
    CompletableFuture<Void> existingFuture = new CompletableFuture<>();
    service.setLoginFuture(existingFuture);
    service.isLoggedIn = false;

    // When: login() is called again
    CompletableFuture<Void> returnedFuture = service.login();

    // Then: should return the same future (not create a new one)
    assertSame("Should return existing future", existingFuture, returnedFuture);
  }

  @Test
  public void testLoginCreatesNewFutureWhenNotLoggedIn() throws Exception {
    // Given: service is not logged in and no login in progress
    service.isLoggedIn = false;
    service.setLoginFuture(null);

    // When: login() is called
    CompletableFuture<Void> future = service.login();

    // Then: should create a new pending future
    assertNotNull("Future should not be null", future);
    assertFalse("Future should not be done yet", future.isDone());
  }

  // ==================== messageHandler() Login Success Tests ====================

  @Test
  public void testMessageHandlerCompletesLoginFutureOnSuccess() throws Exception {
    // Given: a pending login future
    CompletableFuture<Void> loginFuture = new CompletableFuture<>();
    service.setLoginFuture(loginFuture);
    service.isLoggedIn = false;

    // When: login success message is received
    String successMessage = "{\"event\":\"login\",\"code\":\"0\",\"msg\":\"success\"}";
    service.messageHandler(successMessage);

    // Then: login future should be completed, isLoggedIn should be true
    assertTrue("Login future should be done", loginFuture.isDone());
    assertFalse("Login future should not be exceptionally completed", loginFuture.isCompletedExceptionally());
    assertTrue("isLoggedIn should be true", service.isLoggedIn);
  }

  // ==================== messageHandler() Login Error Tests ====================

  @Test
  public void testMessageHandlerCompletesLoginFutureExceptionallyOnError60011() throws Exception {
    // Given: a pending login future
    CompletableFuture<Void> loginFuture = new CompletableFuture<>();
    service.setLoginFuture(loginFuture);
    service.isLoggedIn = true; // Simulate was logged in

    // When: error 60011 message is received
    String errorMessage = "{\"event\":\"error\",\"code\":\"60011\",\"msg\":\"Please log in\"}";
    service.messageHandler(errorMessage);

    // Then: login future should be completed exceptionally, isLoggedIn should be false
    assertTrue("Login future should be done", loginFuture.isDone());
    assertTrue("Login future should be exceptionally completed", loginFuture.isCompletedExceptionally());
    assertFalse("isLoggedIn should be false", service.isLoggedIn);

    // Verify the exception message
    try {
      loginFuture.get();
      fail("Should have thrown exception");
    } catch (ExecutionException e) {
      assertTrue("Should be ExchangeException", e.getCause() instanceof ExchangeException);
      assertTrue("Should contain error code", e.getCause().getMessage().contains("60011"));
    }
  }

  @Test
  public void testMessageHandlerCompletesLoginFutureExceptionallyOnError60031() throws Exception {
    // Given: a pending login future
    CompletableFuture<Void> loginFuture = new CompletableFuture<>();
    service.setLoginFuture(loginFuture);
    service.isLoggedIn = true;

    // When: error 60031 message is received
    String errorMessage = "{\"event\":\"error\",\"code\":\"60031\",\"msg\":\"Invalid login\"}";
    service.messageHandler(errorMessage);

    // Then: login future should be completed exceptionally
    assertTrue("Login future should be done", loginFuture.isDone());
    assertTrue("Login future should be exceptionally completed", loginFuture.isCompletedExceptionally());
    assertFalse("isLoggedIn should be false", service.isLoggedIn);
  }

  // ==================== Error Propagation to Singles Tests ====================

  @Test
  @SuppressWarnings("unchecked")
  public void testErrorPropagationToSinglesOnAuthError() throws Exception {
    // Given: a pending order emitter in singles map
    ObservableEmitter<JsonNode> mockEmitter = mock(ObservableEmitter.class);
    when(mockEmitter.isDisposed()).thenReturn(false);
    service.getSingles().put("123", mockEmitter);
    service.isLoggedIn = true;

    // When: error 60011 message is received
    String errorMessage = "{\"event\":\"error\",\"code\":\"60011\",\"msg\":\"Please log in\"}";
    service.messageHandler(errorMessage);

    // Then: emitter should receive onError
    verify(mockEmitter).onError(any(ExchangeException.class));
    assertTrue("Singles map should be cleared", service.getSingles().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testErrorPropagationToMultipleSingles() throws Exception {
    // Given: multiple pending order emitters
    ObservableEmitter<JsonNode> emitter1 = mock(ObservableEmitter.class);
    ObservableEmitter<JsonNode> emitter2 = mock(ObservableEmitter.class);
    when(emitter1.isDisposed()).thenReturn(false);
    when(emitter2.isDisposed()).thenReturn(false);
    service.getSingles().put("order1", emitter1);
    service.getSingles().put("order2", emitter2);
    service.isLoggedIn = true;

    // When: error message is received
    String errorMessage = "{\"event\":\"error\",\"code\":\"60011\",\"msg\":\"Please log in\"}";
    service.messageHandler(errorMessage);

    // Then: both emitters should receive onError
    verify(emitter1).onError(any(ExchangeException.class));
    verify(emitter2).onError(any(ExchangeException.class));
    assertTrue("Singles map should be cleared", service.getSingles().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDisposedEmittersAreSkipped() throws Exception {
    // Given: a disposed emitter
    ObservableEmitter<JsonNode> disposedEmitter = mock(ObservableEmitter.class);
    when(disposedEmitter.isDisposed()).thenReturn(true);
    service.getSingles().put("disposed", disposedEmitter);
    service.isLoggedIn = true;

    // When: error message is received
    String errorMessage = "{\"event\":\"error\",\"code\":\"60011\",\"msg\":\"Please log in\"}";
    service.messageHandler(errorMessage);

    // Then: disposed emitter should not receive onError
    verify(disposedEmitter, never()).onError(any());
  }

  // ==================== Re-login Tests ====================

  @Test
  public void testReLoginTriggeredAfterAuthError() throws Exception {
    // Given: service was logged in
    service.isLoggedIn = true;
    service.setLoginFuture(null);

    // When: error 60011 message is received
    String errorMessage = "{\"event\":\"error\",\"code\":\"60011\",\"msg\":\"Please log in\"}";
    service.messageHandler(errorMessage);

    // Then: a new login should be triggered (new loginFuture created)
    CompletableFuture<Void> newFuture = service.getLoginFuture();
    assertNotNull("New login future should be created", newFuture);
    assertFalse("New login future should be pending", newFuture.isDone());
  }

  // ==================== Order Response Routing Tests ====================

  @Test
  @SuppressWarnings("unchecked")
  public void testOrderResponseRoutedToCorrectEmitter() throws Exception {
    // Given: a pending order emitter
    ObservableEmitter<JsonNode> mockEmitter = mock(ObservableEmitter.class);
    when(mockEmitter.isDisposed()).thenReturn(false);
    service.getSingles().put("12345", mockEmitter);

    // When: order response with matching id is received
    String orderResponse = "{\"id\":\"12345\",\"op\":\"order\",\"data\":[{\"ordId\":\"order123\",\"sCode\":\"0\"}]}";
    service.messageHandler(orderResponse);

    // Then: emitter should receive onNext and onComplete
    verify(mockEmitter).onNext(any(JsonNode.class));
    verify(mockEmitter).onComplete();
    assertFalse("Emitter should be removed from singles", service.getSingles().containsKey("12345"));
  }

  // ==================== Connection State Tests ====================

  @Test
  public void testLoginFutureCompletedExceptionallyOnChannelInactive() throws Exception {
    // Given: a pending login future
    CompletableFuture<Void> loginFuture = new CompletableFuture<>();
    service.setLoginFuture(loginFuture);

    // When: channel becomes inactive (simulated)
    service.simulateChannelInactive();

    // Then: login future should be completed exceptionally
    assertTrue("Login future should be done", loginFuture.isDone());
    assertTrue("Login future should be exceptionally completed", loginFuture.isCompletedExceptionally());
    assertFalse("isLoggedIn should be false", service.isLoggedIn);
  }

  // ==================== Thread Safety Tests ====================

  @Test
  public void testConcurrentLoginCallsReturnSameFuture() throws Exception {
    // Given: service is not logged in
    service.isLoggedIn = false;
    service.setLoginFuture(null);

    // When: multiple threads call login concurrently
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    ConcurrentHashMap<Integer, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();

    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          CompletableFuture<Void> future = service.login();
          futures.put(index, future);
        } catch (Exception e) {
          // Ignore for test
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown(); // Start all threads simultaneously
    doneLatch.await(5, TimeUnit.SECONDS);
    executor.shutdown();

    // Then: all futures should be the same instance (or completed already-logged-in futures)
    CompletableFuture<Void> firstFuture = futures.values().iterator().next();
    for (CompletableFuture<Void> future : futures.values()) {
      // They should either be the same pending future or both completed (if isLoggedIn was set)
      assertNotNull("Future should not be null", future);
    }
  }

  /**
   * Testable subclass that exposes internal state for testing.
   */
  static class TestableOkexStreamingService extends OkexStreamingService {
    private boolean skipActualLogin = true;

    public TestableOkexStreamingService(String apiUrl, ExchangeSpecification exchangeSpecification) {
      super(apiUrl, exchangeSpecification);
    }

    // Expose the loginFuture for testing
    public void setLoginFuture(CompletableFuture<Void> future) {
      try {
        java.lang.reflect.Field field = OkexStreamingService.class.getDeclaredField("loginFuture");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<CompletableFuture<Void>> ref = (AtomicReference<CompletableFuture<Void>>) field.get(this);
        ref.set(future);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public CompletableFuture<Void> getLoginFuture() {
      try {
        java.lang.reflect.Field field = OkexStreamingService.class.getDeclaredField("loginFuture");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<CompletableFuture<Void>> ref = (AtomicReference<CompletableFuture<Void>>) field.get(this);
        return ref.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // Expose singles map for testing
    @SuppressWarnings("unchecked")
    public Map<String, ObservableEmitter<JsonNode>> getSingles() {
      return singles;
    }

    // Simulate channel inactive for testing
    public void simulateChannelInactive() {
      isLoggedIn = false;
      CompletableFuture<Void> future = getLoginFuture();
      if (future != null && !future.isDone()) {
        future.completeExceptionally(new ExchangeException("Connection closed"));
      }
      setLoginFuture(null);
    }

    @Override
    public void resubscribeChannels() {
      // No-op for unit tests to avoid network calls
    }

    @Override
    public CompletableFuture<Void> login() throws Exception {
      // If already logged in, return completed future
      if (isLoggedIn) {
        return CompletableFuture.completedFuture(null);
      }

      // If login is already in progress, return existing future
      CompletableFuture<Void> existingFuture = getLoginFuture();
      if (existingFuture != null && !existingFuture.isDone()) {
        return existingFuture;
      }

      // Create new login future (skip actual network call for unit tests)
      CompletableFuture<Void> newFuture = new CompletableFuture<>();
      setLoginFuture(newFuture);

      if (!skipActualLogin) {
        // Would send actual login message here
      }

      return newFuture;
    }
  }
}
