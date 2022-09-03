/*
 * Copyright (c) 2017, 2022, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.a.authentication;

import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.Security;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;

import java.util.List;

public class Sm32PasswordPlugin implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "sm32_password";

    protected Protocol<NativePacketPayload> protocol = null;
    protected MysqlCallbackHandler usernameCallbackHandler = null;
    protected String password = null;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        this.protocol = prot;
        this.usernameCallbackHandler = cbh;

    }

    public void destroy() {
        reset();
        this.protocol = null;
        this.usernameCallbackHandler = null;
        this.password = null;
    }

    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    public boolean requiresConfidentiality() {
        return false;
    }

    public boolean isReusable() {
        return true;
    }

    public void setAuthenticationParameters(String user, String password) {
        this.password = password;
        if (user == null && this.usernameCallbackHandler != null) {
            // Fall back to system login user.
            this.usernameCallbackHandler.handle(new UsernameCallback(System.getProperty("user.name")));
        }
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (this.password == null || this.password.length() == 0 || fromServer == null) {
            // no password
            NativePacketPayload packet = new NativePacketPayload(new byte[] { 0 });
            toServer.add(packet);

        } else {
            try {
//                if (this.stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
//                    // send a scramble for fast auth
//                    this.seed = fromServer.readString(StringSelfDataType.STRING_TERM, null);
//                    toServer.add(new NativePacketPayload(encryptPassword()));
//                    this.stage = AuthStage.FAST_AUTH_READ_RESULT;
//                    return true;
//                } else if (this.stage == AuthStage.FAST_AUTH_READ_RESULT) {
//                    int fastAuthResult = fromServer.readBytes(StringLengthDataType.STRING_FIXED, 1)[0];
//                    switch (fastAuthResult) {
//                        case 3:
//                            this.stage = AuthStage.FAST_AUTH_COMPLETE;
//                            return true;
//                        case 4:
//                            this.stage = AuthStage.FULL_AUTH;
//                            break;
//                        default:
//                            throw ExceptionFactory.createException("Unknown server response after fast auth.", this.protocol.getExceptionInterceptor());
//                    }
//                }

                // We must request the public key from the server to encrypt the password
                if (fromServer.getPayloadLength() > 0) { // auth data is null terminated
                    // read key response
                    NativePacketPayload packet = new NativePacketPayload(encryptPassword());
                    toServer.add(packet);
                } else {
                    // build and send Public Key Retrieval packet
                    NativePacketPayload packet = new NativePacketPayload(new byte[] { 0 }); //
                    toServer.add(packet);
                }
            } catch (CJException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.protocol.getExceptionInterceptor());
            }
        }
        return true;
    }

    protected byte[] encryptPassword(){
        byte[] input = this.password != null
                ? StringUtils.getBytes(this.password, this.protocol.getServerSession().getCharsetSettings().getPasswordCharacterEncoding())
                : new byte[] { 0 };
        byte[] hashArray = Security.scrambleSm3(input);
        System.out.println("copy CachingSha2PasswordPlugin implements Sm3PasswordPlugin verify : " + Security.verify(input,hashArray) + " src :" + this.password + " hash : " + ByteUtils.toHexString(hashArray));
        return hashArray;
    }
}
