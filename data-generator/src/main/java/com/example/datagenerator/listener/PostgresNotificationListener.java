package com.example.datagenerator.listener;

import com.example.datagenerator.config.PostgresListenerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * {@code PostgresNotificationListener} - Компонент для обрабоки события PostgreSQL LISTEN/NOTIFY канала.
 * <p>
 *     Основные задачи:
 *     <ul>
 *         <li>Устанавливает соединение с PostgreSQL</li>
 *         <li>Асинхронно получает уведомления через {@link PGConnection#getNotifications()}</li>
 *         <li>Доставляет уведомления подписчикам {@link PostgresEventSubscriber} в отдельном пуле потоков.</li>
 *         <li>Автоматически перезапускается при ошибках соединения.</li>
 *     </ul>
 *
 * Конфигурация задаётся через {@link PostgresListenerProperties}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PostgresNotificationListener implements ApplicationListener<ContextRefreshedEvent> {
    private final DataSource dataSource; // Источник данных
    private final PostgresListenerProperties properties; // Конфигурация
    private final ApplicationContext applicationContext; // Контекст приложения для сканирования бинов

    private volatile Connection connection; // JDBC соединение
    private final AtomicBoolean running = new AtomicBoolean(false); // Флаг работы listener'a
    private final AtomicBoolean listenerActive = new AtomicBoolean(false); // Флаг активности
    private final ReentrantLock connectionLock = new ReentrantLock(); // Блокировка потока

    private ScheduledExecutorService listenerExecutor; // Планировщик для запуска listener'а
    private ThreadPoolExecutor subscriberExecutor; // Пул потоков для обработки уведомлений подписчиками

    // Список аннотированных методов
    private List<Map.Entry<Object, Method>> eventHandlers;

    // Проверка имени канала
    private static final Pattern CHANNEL_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    /**
     * Инициализация listener'a:
     * <ul>
     *     <li>Создает отдельный поток для слушателя.</li>
     *     <li>Настраивает пул потоков для подписчиков.</li>
     *     <li>Сканирует и регистрирует обработчики событий.</li>
     *     <li>Запускает listener.</li>
     * </ul>
     */
    @PostConstruct // Инициализация
    public void init() {
        listenerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "postgres-listener-thread");
            thread.setDaemon(true);
            return thread;
        }); // Создание отдельного потока для listener'а

        // Пул потоков для подписчиков
        subscriberExecutor = new ThreadPoolExecutor(
                properties.getSubscriberThreads(),
                properties.getSubscriberThreads(),
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getSubscriberQueueCapacity()),
                new ThreadPoolExecutor.CallerRunsPolicy());

        running.set(true);
        startListener(0); // Запускаем listener
        log.info("PostgresNotificationListener initialized");
    }

    /**
     * Сканирует все бины в контексте приложения и регистрирует методы с аннотацией @PostgresEventHandler.
     */
    private void registerEventHandlers() {
        var beans = applicationContext.getBeansOfType(Object.class).values();
        log.info("Scanning {} beans for event handlers", beans.size());
        eventHandlers = beans.stream()
                .flatMap(bean -> {
                    Class<?> targetClass = AopUtils.getTargetClass(bean);
                    Method[] methods = targetClass.getMethods();
                    return java.util.Arrays.stream(methods)
                            .filter(method -> method.isAnnotationPresent(PostgresEventHandler.class))
                            .peek(method -> log.info("Found event handler: {} in bean {}", method.getName(), bean.getClass().getSimpleName()))
                            .map(method -> Map.entry(bean, method));
                })
                .collect(Collectors.toList());
        log.info("Total event handlers registered: {}", eventHandlers.size());
    }

    /**
     * Регистрирует обработчики событий после полной инициализации контекста приложения.
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        registerEventHandlers();
        log.info("Registered {} event handlers", eventHandlers.size());
    }

    /**
     * Планируем запуск listener'a через указаную задержку
     *
     * @param initialDelayMs задержка в милисекундах
     */
    private void startListener(long initialDelayMs) {
        if (!running.get() || listenerActive.get()) return;
        listenerExecutor.schedule(this::runListener, initialDelayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Устанавливает соединение с PostgreSQL и выполняет LISTEN.
     *
     * @throws SQLException если соединение или LISTEN не удалось
     */
    private void establishConnection() throws SQLException {
        connectionLock.lock(); // Блокируем доступ к соединению

        try {
            // Если соединение уже было открыто то ничего не делаем
            if (connection != null && !connection.isClosed()) return;

            Connection conn = dataSource.getConnection(); // Создаем новое соединение
            String channel = properties.getChannel(); // Получаем из конфигурации имя канала

            if(!CHANNEL_NAME_PATTERN.matcher(channel).matches()) // Валидируем имя канала
                throw new IllegalArgumentException("Invalid channel name: " + channel);

            // Подписываемся на канал
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("LISTEN \"" + channel + "\"");
                log.info("Subscribed to channel: {}", channel);
            }

            this.connection = conn; // Сохраняем соединение
        } finally {
            connectionLock.unlock(); // Выполняем разблокировку
        }
    }

    /**
     * Основной цикл listener'а:
     * <ul>
     *     <li>Получает уведомления через {@link PGConnection#getNotifications()}.</li>
     *     <li>Доставляет их подписчикам.</li>
     *     <li>Проверяет валидность соединения.</li>
     *     <li>Перезапускается при ошибках.</li>
     * </ul>
     */
    private void runListener() {
        if (!running.get()) return; // Если не запущен то ничего не делаем
        listenerActive.set(true); // Отмечаем listener как активный

        try {
            establishConnection(); // Устанавливаем соединение
            PGConnection pgConnection = connection.unwrap(PGConnection.class);

            // Получаем уведомления в цикле
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                PGNotification[] notifications = pgConnection.getNotifications(0);

                // Если получили уведомления, то отправляем их подписавшимся на событие
                if (notifications != null)
                    for (PGNotification notification : notifications)
                        dispatchNotification(notification.getParameter());

                // Проверяем соединение
                if (connection.isClosed() || !connection.isValid(2))
                    throw new SQLException("Connection invalid");
            }
        } catch (Exception e) {
            if (running.get()) { // Если случилась ошибка то перезапускаем listener
                log.error("Listener error, restarting: {}", e.getMessage(), e);
                restartListener();
            }
        } finally {
            listenerActive.set(false); // Выставляем флаг активности
            cleanupConnection(); // Очищаем соединение
        }
    }

    /**
     * Отправляет уведомление всем зарегистрированным обработчикам.
     *
     * @param payload содержимое уведомления
     */
    private void dispatchNotification(String payload) {
        //log.info("Received payload: {}", payload); // Логируем полученный payload

        // Для каждого зарегистрированного обработчика запускаем задачу в пуле потоков
        for (Map.Entry<Object, Method> handler : eventHandlers) {
            subscriberExecutor.execute(() -> {
                try {
                    handler.getValue().invoke(handler.getKey(), payload);
                } catch (Exception e) {
                    log.error("Error invoking event handler {}: {}", handler.getValue().getName(), e.getMessage(), e);
                }
            });
        }
    }


    // Делаем задержку и перезапускаем listener
    private void restartListener() {
        cleanupConnection();
        listenerActive.set(false);
        startListener(properties.getRestartDelayMs());
    }

    // Закрываем соединение
    private void cleanupConnection() {
        connectionLock.lock();
        try {
            if (connection == null) return;

            Connection toClose = connection;
            connection = null;

            if (toClose != null && !toClose.isClosed()) {
                toClose.close();
                log.debug("Postgres connection closed successfully");
            }

        } catch (SQLException e) {
            log.warn("Error closing Postgres connection", e);
        } finally {
            connectionLock.unlock();
        }
    }

    // Останавливаем listener
    private void stop() {
        running.set(false);
        listenerActive.set(false);
        cleanupConnection();
    }

    @PreDestroy // Деинециализируем все
    public void destroy() {
        log.info("Shutting down PostgresNotificationListener");
        stop();

        if (listenerExecutor != null)
            listenerExecutor.shutdownNow();

        if (subscriberExecutor != null) {
            subscriberExecutor.shutdownNow();
            try {
                if (!subscriberExecutor.awaitTermination(10, TimeUnit.SECONDS))
                    subscriberExecutor.shutdownNow();
            } catch (InterruptedException e) {
                subscriberExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("PostgresNotificationListener destroyed");
    }
}
