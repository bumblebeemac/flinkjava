package gaming;

import java.time.Instant;
import java.util.UUID;

public class LookingForGame implements ServerEvent {
  private Instant eventTime;
  private UUID playerId;
  private GameType gameType;

  public LookingForGame(Instant eventTime, UUID playerId, GameType gameType) {
    this.eventTime = eventTime;
    this.playerId = playerId;
    this.gameType = gameType;
  }

  @Override
  public Instant getEventTime() {
    return eventTime;
  }

  public UUID getPlayerId() {
    return playerId;
  }

  @Override
  public String getId() {
    return "player|" + playerId;
  }
}
