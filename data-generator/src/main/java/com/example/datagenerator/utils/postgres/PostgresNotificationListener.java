package com.example.datagenerator.utils.postgres;

import com.example.datagenerator.config.PostgresListenerProperties;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

@Slf4j
@Component
public class PostgresNotificationListener {
    // Регулярка для валидации имени канала
    private static final Pattern CHANNEL_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]*$");

    private final DataSource dataSource; // Источники соединений с PostgreSQL
    private final PostgresListenerProperties properties; // Настройки
    private final ApplicationContext applicationContext; // Нужен для поиска бинов обработчиков

    private final TaskExecutor taskExecutor; // Пул потоков для асинхронного вызова обработчиков

    private Connection connection; // JDBC-соединение, через которое выполняется LISTEN запрос

    // Список зарегистрированных обработчиков
    private final List<Map.Entry<Object, Method>> eventHandlers = new CopyOnWriteArrayList<>();

    public PostgresNotificationListener(
            DataSource dataSource, PostgresListenerProperties properties,
            ApplicationContext applicationContext,
            @Qualifier("subscriberExecutor") TaskExecutor taskExecutor) {
        this.dataSource = dataSource;
        this.properties = properties;
        this.applicationContext = applicationContext;
        this.taskExecutor = taskExecutor;
    }

    @PostConstruct // При старте приложения устанавливаем соединение
    public void init() {
        log.info("Initializing PostgresNotificationListener");
        establishConnection();
    }

    @PreDestroy // Закрываем соединение при завершении работы приложения
    public void destroy() {
        log.info("Shutting down PostgresNotificationListener");
        cleanupConnection();
    }

    // После инициализации контекста проходимся по всем бинам и регистрируем обработчики
    @EventListener(ContextRefreshedEvent.class)
    public void registerEventHandlers() {
        var beans = applicationContext.getBeansOfType(Object.class).values();
        beans.forEach(bean -> {
            for (var method : bean.getClass().getMethods()) {
                if (method.isAnnotationPresent(PostgresEventHandler.class)) {
                    eventHandlers.add(Map.entry(bean, method));
                    log.info("Registered handler {} in {}",
                            method.getName(), bean.getClass().getSimpleName());
                }
            }
        });
    }

    // Устанавливаем JBDC соединение и выполняем запрос
    private void establishConnection() {
        try {
            connection = dataSource.getConnection();
            String channel = properties.getChannel();

            // Валидируем имя канала
            if (!CHANNEL_NAME_PATTERN.matcher(channel).matches())
                throw new IllegalArgumentException("Invalid channel name: " + channel);

            // Выполняем запрос для получения сообщений
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("LISTEN \"" + channel + "\"");
                log.info("Subscribed to channel: {}", channel);
            }
        } catch (SQLException e) {
            log.error("Failed to establish connection", e);
        }
    }

    // Закрываем соединение, если оно открыто
    private void cleanupConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                log.info("Connection closed");
            }
        } catch (SQLException e) {
            log.warn("Error closing connection", e);
        }
    }

    // Этот метод вызывается каждые N миллисекунд и обрабатывает новые сообщения
    @Scheduled(fixedDelayString = "#{${postgres.listener.poll-interval-ms:1000}}")
    public void pollNotifications() {
        // Если соединение отсутствует пытаемся восстановить
        if (connection == null) {
            establishConnection();
            return;
        }

        try {
            // Разворачиваем JDBC соединение в PGConnection
            PGConnection pgConnection = connection.unwrap(PGConnection.class);

            // Получает массив уведомлений
            PGNotification[] notifications = pgConnection.getNotifications();

            // Для каждого уведомления вызываем dispatcher
            if (notifications != null) {
                for (PGNotification notification : notifications)
                    dispatchNotification(notification.getParameter());
            }
        } catch (Exception e) {
            log.error("Error polling notifications, reconnecting", e);
            cleanupConnection();
            establishConnection();
        }
    }

    // Для каждого зарегестрированого обработчика создаем задачу в пуле потоков
    private void dispatchNotification(String payload) {
        eventHandlers.forEach(handler -> taskExecutor.execute(() -> {
            try {
                handler.getValue().invoke(handler.getKey(), payload);
            } catch (Exception e) {
                log.error("Error invoking handler {}: {}", handler.getValue().getName(), e.getMessage(), e);
            }
        }));
    }
}
