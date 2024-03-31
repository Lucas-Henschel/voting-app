package infrastructure.lifecycle;

import domain.Election;
import infrastructure.repositories.RedisElectionRepository;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.logging.Logger;

@Startup
@ApplicationScoped
public class Subscribe {
  private static final Logger LOGGER = Logger.getLogger(String.valueOf(Subscribe.class));

  public Subscribe(ReactiveRedisDataSource dataSource, RedisElectionRepository repository) {
    LOGGER.info("Startup: Subscribe");

    dataSource.pubsub(String.class)
      .subscribe("elections")
      .emitOn(Infrastructure.getDefaultWorkerPool())
      .subscribe()
      .with(id -> {
        LOGGER.info("Election " + id + " received from subscription");
        LOGGER.info("Election " + repository.findById(id) + " starting");
      });
  }
}
