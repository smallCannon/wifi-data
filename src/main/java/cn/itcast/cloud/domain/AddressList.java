package cn.itcast.cloud.domain;

public class AddressList {

	//{"type":"street","status":1,"name":"海淀南路","admCode":"110108","admName":"北京市,海淀区","addr":"","nearestPoint":[116.30416,39.97561],"distance":87.995}
	private String type;
	private String status;
	private String name;
	private String admCode;
	private String admName;
	private String addr;
	private String[] nearestPoint;
	private String distance;
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAdmCode() {
		return admCode;
	}
	public void setAdmCode(String admCode) {
		this.admCode = admCode;
	}
	public String getAdmName() {
		return admName;
	}
	public void setAdmName(String admName) {
		this.admName = admName;
	}
	public String getAddr() {
		return addr;
	}
	public void setAddr(String addr) {
		this.addr = addr;
	}
	public String[] getNearestPoint() {
		return nearestPoint;
	}
	public void setNearestPoint(String[] nearestPoint) {
		this.nearestPoint = nearestPoint;
	}
	public String getDistance() {
		return distance;
	}
	public void setDistance(String distance) {
		this.distance = distance;
	}
	

}
