package com.cor.cep.event;

/**
 * Class MotorbikeEvent
 * Immutable Motorbike Event class. The process control system creates these
 * events. The MotorbikeEventHandler picks these up and processes them.
 */
public class MotorbikeEvent {

	private long timestamp; // Timestamp in milliseconds.
	private Integer motorbikeId;
	private String location;
	private Double speed;
	private Double tirePressure1;
	private Double tirePressure2;
	private Boolean seat;

	/**
	 * Motorbike constructor.
	 */
	public MotorbikeEvent(long timestamp, int motorbikeId, String location, Double speed, Double tirePressure1,
			Double tirePressure2, Boolean seat) {
		this.setTimestamp(timestamp);
		this.setMotorbikeId(motorbikeId);
		this.setLocation(location);
		this.setSpeed(speed);
		this.setTirePressure1(tirePressure1);
		this.setTirePressure2(tirePressure2);
		this.setSeat(seat);
	}

	@Override
	public String toString() {
		return "MotorbikeEvent [" + timestamp + "," + motorbikeId + "," + location + "," + speed + "," + tirePressure1
				+ "," + tirePressure2 + "," + seat + "]";
	}

	/**
	 * Getter and setter
	 */

	public Boolean getSeat() {
		return seat;
	}

	public void setSeat(Boolean seat) {
		this.seat = seat;
	}

	public Double getTirePressure2() {
		return tirePressure2;
	}

	public void setTirePressure2(Double tirePressure2) {
		this.tirePressure2 = tirePressure2;
	}

	public Double getTirePressure1() {
		return tirePressure1;
	}

	public void setTirePressure1(Double tirePressure1) {
		this.tirePressure1 = tirePressure1;
	}

	public Double getSpeed() {
		return speed;
	}

	public void setSpeed(Double speed) {
		this.speed = speed;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Integer getMotorbikeId() {
		return motorbikeId;
	}

	public void setMotorbikeId(Integer motorbikeId) {
		this.motorbikeId = motorbikeId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
