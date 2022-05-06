package org.summerclouds.common.jms;

import javax.jms.Message;

import org.summerclouds.common.core.security.ISubject;

public interface TrustApi {

	String createToken(String string, Message msg, ISubject subject);

}
