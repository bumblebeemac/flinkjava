package gaming;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class Player {
  private UUID playerId;
  private String nickname;

  public Player(UUID playerId, String nickname) {
    this.playerId = playerId;
    this.nickname = nickname;
  }

  public UUID getPlayerId() {
    return playerId;
  }

  public String getNickname() {
    return nickname;
  }

  public PlayerRegistered register(Instant startTime, Duration duration) {
    return new PlayerRegistered(startTime.plus(duration), playerId, nickname);
  }

  public PlayerOnline online(Instant startTime, Duration duration) {
    return new PlayerOnline(startTime.plus(duration), playerId, nickname);
  }

  public PlayerOffline offline(Instant startTime, Duration duration) {
    return new PlayerOffline(startTime.plus(duration), playerId, nickname);
  }

  public LookingForGame lookForGame(Instant startTime, Duration duration, GameType gameType) {
    return new LookingForGame(startTime.plus(duration), playerId, gameType);
  }
}
