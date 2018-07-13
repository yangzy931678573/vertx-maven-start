package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;

import io.vertx.source.WikiDatabaseVerticle;
import io.vertx.source.HttpServerVerticle;
/**
 * Created by Administrator on 2018/1/19.
 * Description:
 */
public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        System.out.println("this is redeploy !!!");
        Future<String> dbDeployment = Future.future();
        vertx.deployVerticle(new WikiDatabaseVerticle(), dbDeployment.completer());


        dbDeployment.compose(id -> {
            Future<String> deployment = Future.future();
            vertx.deployVerticle("io.vertx.source.HttpServerVerticle",
                    new DeploymentOptions().setInstances(2),
                    deployment.completer());

            return deployment;

        }).setHandler(ar -> {
            if (ar.succeeded())
                startFuture.complete();
            else
                startFuture.fail(ar.cause());

        });
    }
}
