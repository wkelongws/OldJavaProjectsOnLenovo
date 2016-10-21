package bean;

public class Lane {
	
	private String laneID;
	private String count;
	private String volume;
	private String occupancy;
	private String speed;
	
	private String smallCount;
	private String smallVolume;
	private String mediumCount;
	private String mediumVolume;
	private String largeCount;
	private String largeVolume;
	
	public String getLaneID() {
		return laneID;
	}
	public void setLaneID(String laneID) {
		this.laneID = laneID;
	}
	public String getCount() {
		return count;
	}
	public void setCount(String count) {
		this.count = count;
	}
	public String getVolume() {
		return volume;
	}
	public void setVolume(String volume) {
		this.volume = volume;
	}
	public String getOccupancy() {
		return occupancy;
	}
	public void setOccupancy(String occupancy) {
		this.occupancy = occupancy;
	}
	public String getSpeed() {
		return speed;
	}
	public void setSpeed(String speed) {
		this.speed = speed;
	}
	public String getSmallCount() {
		return smallCount;
	}
	public void setSmallCount(String smallCount) {
		this.smallCount = smallCount;
	}
	public String getSmallVolume() {
		return smallVolume;
	}
	public void setSmallVolume(String smallVolume) {
		this.smallVolume = smallVolume;
	}
	public String getMediumCount() {
		return mediumCount;
	}
	public void setMediumCount(String mediumCount) {
		this.mediumCount = mediumCount;
	}
	public String getMediumVolume() {
		return mediumVolume;
	}
	public void setMediumVolume(String mediumVolume) {
		this.mediumVolume = mediumVolume;
	}
	public String getLargeCount() {
		return largeCount;
	}
	public void setLargeCount(String largeCount) {
		this.largeCount = largeCount;
	}
	public String getLargeVolume() {
		return largeVolume;
	}
	public void setLargeVolume(String largeVolume) {
		this.largeVolume = largeVolume;
	}
	
	@Override
	public String toString() {
		return laneID + "," + count + "," + volume + "," + occupancy + ","
				+ speed + "," + smallCount + "," + smallVolume + ","
				+ mediumCount + "," + mediumVolume + "," + largeCount + ","
				+ largeVolume;
	}
}
