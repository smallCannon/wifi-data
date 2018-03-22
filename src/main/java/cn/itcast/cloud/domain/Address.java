package cn.itcast.cloud.domain;

public class Address {
//{"queryLocation":[39.97646,116.3039],"addrList":[{"type":"street","status":1,"name":"海淀南路","admCode":"110108","admName":"北京市,海淀区","addr":"","nearestPoint":[116.30416,39.97561],"distance":87.995}]}
	private String[] queryLocation;
	private AddressList[] addrList;
	
	public String[] getQueryLocation() {
		return queryLocation;
	}
	public void setQueryLocation(String[] queryLocation) {
		this.queryLocation = queryLocation;
	}
	public AddressList[] getAddrList() {
		return addrList;
	}
	public void setAddrList(AddressList[] addrList) {
		this.addrList = addrList;
	}
	
	
	
}
