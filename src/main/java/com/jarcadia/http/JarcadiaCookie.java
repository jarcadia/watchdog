package com.jarcadia.http;

import java.lang.invoke.MethodHandles;
import java.util.Date;

import org.apache.http.cookie.Cookie;

import com.jarcadia.rcommando.proxy.Proxy;

public interface JarcadiaCookie extends Cookie, Proxy {
	
	public static MethodHandles.Lookup createLookup() {
		return MethodHandles.lookup();
	}

	@Override
    String getName();

	@Override
	String getValue();

	@Override
	Date getExpiryDate();

	@Override
	boolean isPersistent();

	@Override
	String getDomain();

	@Override
	String getPath();

	@Override
	boolean isSecure();

	@Override
	default boolean isExpired(Date date) {
		Date expiration = this.getExpiryDate();
		return expiration == null ? false : date.after(expiration);
	}
	
	/* The following methods are marked @Obsolete in Cookie and should not be used */

	@Override
	String getComment();

	@Override
	String getCommentURL();

	@Override
	int getVersion();

	@Override
	int[] getPorts();
	
	
	/* Setters */
	void setValues(String domain, String path, String name, String value, int version, boolean persistent, boolean secure);
}
