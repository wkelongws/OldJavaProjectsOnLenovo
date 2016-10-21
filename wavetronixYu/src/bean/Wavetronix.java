package bean;

import java.util.List;

public class Wavetronix {
	
	private String detectorID;
	private String date;
	private String startTime;
	private String endTime;
	private String status;
	private int numOfLanes;
	private List<Lane> laneList;
	
	// Constructor
	public Wavetronix(String detectorID, String date, String startTime,
			String endTime, String status, int numOfLanes, List<Lane> laneList) {
		this.detectorID = detectorID;
		this.date = date;
		this.startTime = startTime;
		this.endTime = endTime;
		this.status = status;
		this.numOfLanes = numOfLanes;
		this.laneList = laneList;
	}
	
	public String getDetectorID() {
		return detectorID;
	}
	
	public void setDetectorID(String detectorID) {
		this.detectorID = detectorID;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public int getNumOfLanes() {
		return numOfLanes;
	}
	public void setNumOfLanes(int numOfLanes) {
		this.numOfLanes = numOfLanes;
	}
	public List<Lane> getLaneList() {
		return laneList;
	}
	public void setLaneList(List<Lane> laneList) {
		this.laneList = laneList;
	}
	
	public String toString() {
		String str = detectorID + "," + date + "," + startTime + "," + endTime + "," + status + "," + numOfLanes;
		for (Lane l : laneList) {
			str = str + "," + l.toString();
		}
		return str;
	}
}
