package infrastructure.repositories;

import domain.Candidate;
import domain.Election;
import domain.ElectionRepository;
import io.quarkus.cache.CacheResult;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.sortedset.SortedSetCommands;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.logging.Logger;

@ApplicationScoped
public class RedisElectionRepository implements ElectionRepository {
  private static final Logger LOGGER = Logger.getLogger(String.valueOf(RedisElectionRepository.class));
  private final SortedSetCommands<String, String> sortedSetCommands;
  private static final String KEY = "election:";
  private final KeyCommands<String> keyCommands;

  public RedisElectionRepository(RedisDataSource redisDataSource) {
    sortedSetCommands = redisDataSource.sortedSet(String.class, String.class);
    keyCommands = redisDataSource.key(String.class);
  }

  @Override
  @CacheResult(cacheName = "memoization")
  public Election findById(String id) {
    LOGGER.info("Retrieving election " + id + " from redis");

    return new Election(id, sortedSetCommands.zrange(KEY + id, 0, -1)
      .stream()
      .map(Candidate::new)
      .toList());
  }

  @Override
  public List<Election> findAll() {
    LOGGER.info("Retroeving elections from redis");
    return keyCommands.keys("election:*")
      .stream()
      .map(id -> findById(id.replace(KEY, "")))
      .toList();
  }

  @Override
  public void vote(String electionId, Candidate candidate) {
    LOGGER.info("Voting for " + candidate.id());
    sortedSetCommands.zincrby(KEY + electionId, 1, candidate.id());
  }
}
