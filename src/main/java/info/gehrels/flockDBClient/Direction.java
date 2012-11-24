package info.gehrels.flockDBClient;

public enum Direction {
	INCOMING(false), OUTGOING(true);

	final boolean forward;

	Direction(boolean forward) {
		this.forward = forward;
	}
}
