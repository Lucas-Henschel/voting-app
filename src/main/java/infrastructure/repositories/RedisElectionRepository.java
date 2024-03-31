package infrastructure.repositories;

import domain.Candidate;
import domain.Election;
import domain.ElectionRepository;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.sortedset.SortedSetCommands;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.logging.Logger;

@ApplicationScoped
public class RedisElectionRepository implements ElectionRepository {
  private static final Logger LOGGER = Logger.getLogger(String.valueOf(RedisElectionRepository.class));
  private final SortedSetCommands<String, String> sortedSetCommands;
  private static final String KEY = "election:";

  public RedisElectionRepository(RedisDataSource redisDataSource) {
    sortedSetCommands = redisDataSource.sortedSet(String.class, String.class);
  }

  @Override
  public Election findById(String id) {
    LOGGER.info("Retrieving election " + id + " from redis");

    return new Election(id, sortedSetCommands.zrange(KEY + id, 0, -1)
      .stream()
      .map(Candidate::new)
      .toList());
  }
}
