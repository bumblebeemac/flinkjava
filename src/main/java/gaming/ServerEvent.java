package gaming;

import java.time.Instant;

public interface ServerEvent {
  Instant getEventTime();
  String getId();
}



