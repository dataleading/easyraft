package com.dataleading.jraft;


/**
 * @author shshen
 *
 */
public class EndPoint {

	private String hostname;
	private int port;
	
	public EndPoint(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}
	
	public String getHostname() {
		return hostname;
	}
	public int getPort() {
		return port;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EndPoint other = (EndPoint) obj;
		if (hostname == null) {
			if (other.hostname != null)
				return false;
		} else if (!hostname.equals(other.hostname))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return hostname + ":" + port;
	}

}
