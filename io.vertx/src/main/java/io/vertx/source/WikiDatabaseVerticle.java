package io.vertx.source;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2018/2/2.
 */
public class WikiDatabaseVerticle extends AbstractVerticle {
    private SQLClient mysqlClient;

    public static final String CONFIG_WIKI_DB_MYSQL_HOST = "wikiDb.mysql.host";
    public static final String CONFIG_WIKI_DB_MYSQL_PORT = "wikiDb.mysql.port";
    public static final String CONFIG_WIKI_DB_MYSQL_USERNAME = "wikiDb.mysql.username";
    public static final String CONFIG_WIKI_DB_MYSQL_PASSWORD = "wikiDb.mysql.password";
    public static final String CONFIG_WIKI_DB_MYSQL_DATABASE = "wikiDb.mysql.database";
    public static final String CONFIG_WIKI_DB_MYSQL_MAX_POOL_SIZE = "wikiDb.mysql.maxPoolSize";

    public static final String CONFIG_WIKI_DB_SQL_QUERIES_RESOURCE_FILE = "wikiDb.sqlQueries.resource.file";
    public static final String CONFIG_WIKI_DB_QUEUE = "wikiDb.queue";


    private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        loadSqlQueries();

        mysqlClient = MySQLClient.createShared(vertx, new JsonObject()
                .put("host", config().getString(CONFIG_WIKI_DB_MYSQL_HOST, "localhost"))
                .put("port", config().getInteger(CONFIG_WIKI_DB_MYSQL_PORT, 3306))
                .put("username", config().getString(CONFIG_WIKI_DB_MYSQL_USERNAME, "root"))
                .put("password", config().getString(CONFIG_WIKI_DB_MYSQL_PASSWORD, "Labbook_701"))
                .put("database", config().getString(CONFIG_WIKI_DB_MYSQL_DATABASE, "testDb"))
                .put("maxPoolSize", config().getInteger(CONFIG_WIKI_DB_MYSQL_MAX_POOL_SIZE, 10)));

        mysqlClient.getConnection(ar -> {
            if (ar.failed()) {
                LOGGER.error("Could not open a database connection", ar.cause());
                startFuture.fail(ar.cause());
            } else {
                SQLConnection connection = ar.result();
                connection.execute(QuerySql.CREATE_PAGES_TABLE.sql(), asyncResult -> {
                    connection.close();
                    if (asyncResult.failed()) {
                        LOGGER.error("Database preparation error", asyncResult.cause());
                        startFuture.fail(asyncResult.cause());
                    } else {
                        vertx.eventBus().consumer(config().getString(CONFIG_WIKI_DB_QUEUE, "wikiDb.queue"), this::onMessage);
                        startFuture.complete();
                    }
                });
            }
        });
    }

    private void onMessage(Message<JsonObject> message) {
        if (!message.headers().contains("action")) {
            LOGGER.error("No action header specified for message with headers {} and body {}",
                    message.headers(), message.body().encodePrettily());
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
            return;
        }
        String action = message.headers().get("action");

        switch (action) {
            case "all-pages":
                fetchAllPages(message);
                break;
            case "get-page":
                fetchPage(message);
                break;
            case "create-page":
                createPage(message);
                break;
            case "save-page":
                savePage(message);
                break;
            case "delete-page":
                deletePage(message);
                break;
            default:
                message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action);
        }
    }

    private void deletePage(Message<JsonObject> message) {

        JsonArray data = new JsonArray().add(message.body().getString("id"));

        mysqlClient.updateWithParams(QuerySql.DELETE_PAGE.sql(), data, res -> {
            if (res.succeeded()) {
                message.reply("ok");
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }

    private void fetchAllPages(Message<JsonObject> message) {
        mysqlClient.query(QuerySql.ALL_PAGES.sql(), res -> {
            if (res.succeeded()) {
                List<String> pages = res.result()
                        .getResults()
                        .stream()
                        .map(json -> json.getString(0))
                        .sorted()
                        .collect(Collectors.toList());
                message.reply(new JsonObject().put("pages", new JsonArray(pages)));
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }

    private void fetchPage(Message<JsonObject> message) {
        String requestedPage = message.body().getString("page");
        JsonArray params = new JsonArray().add(requestedPage);

        mysqlClient.queryWithParams(QuerySql.GET_PAGE.sql(), params, fetch -> {
            if (fetch.succeeded()) {
                JsonObject response = new JsonObject();
                ResultSet resultSet = fetch.result();
                if (resultSet.getNumRows() == 0) {
                    response.put("found", false);
                } else {
                    response.put("found", true);
                    JsonArray row = resultSet.getResults().get(0);
                    response.put("id", row.getInteger(0));
                    response.put("rawContent", row.getString(1));
                }
                message.reply(response);
            } else {
                reportQueryError(message, fetch.cause());
            }
        });
    }

    private void createPage(Message<JsonObject> message) {
        JsonObject request = message.body();
        JsonArray data = new JsonArray()
                .add(request.getString("title"))
                .add(request.getString("markdown"));

        mysqlClient.updateWithParams(QuerySql.CREATE_PAGE.sql(), data, res -> {
            if (res.succeeded()) {
                message.reply("ok");
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }


    private void savePage(Message<JsonObject> message) {
        JsonObject request = message.body();
        JsonArray data = new JsonArray()
                .add(request.getString("markdown"))
                .add(request.getString("id"));

        mysqlClient.updateWithParams(QuerySql.SAVE_PAGE.sql(), data, res -> {
            if (res.succeeded()) {
                message.reply("ok");
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }
    private void reportQueryError(Message<JsonObject> message, Throwable cause) {
        LOGGER.error("Database query error", cause);
        message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
    }

    //加载sql语句
    private void loadSqlQueries() throws IOException {

        String queriesFile = config().getString(CONFIG_WIKI_DB_SQL_QUERIES_RESOURCE_FILE);
        InputStream queriesInputStream;
        if (queriesFile != null) {
            queriesInputStream = new FileInputStream(queriesFile);
        } else {
            queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
        }
        Properties queriesProps = new Properties();
        queriesProps.load(queriesInputStream);
        queriesInputStream.close();

        QuerySql.CREATE_PAGES_TABLE.setSql(queriesProps.getProperty("create-pages-table"));
        QuerySql.ALL_PAGES.setSql(queriesProps.getProperty("all-pages"));
        QuerySql.GET_PAGE.setSql(queriesProps.getProperty("get-page"));
        QuerySql.CREATE_PAGE.setSql(queriesProps.getProperty("create-page"));
        QuerySql.SAVE_PAGE.setSql(queriesProps.getProperty("save-page"));
        QuerySql.DELETE_PAGE.setSql(queriesProps.getProperty("delete-page"));
    }

}
