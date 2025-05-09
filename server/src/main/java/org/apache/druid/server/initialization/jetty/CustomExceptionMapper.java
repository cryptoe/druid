/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.initialization.jetty;


import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class CustomExceptionMapper implements ExceptionMapper<JsonMappingException>
{
  private static final Logger log = new Logger(CustomExceptionMapper.class);
  public static final String ERROR_KEY = "error";
  public static final String UNABLE_TO_PROCESS_ERROR = "unknown json mapping exception";

  @Override
  public Response toResponse(JsonMappingException exception)
  {
    log.warn(exception.getMessage() == null ? UNABLE_TO_PROCESS_ERROR : exception.getMessage());
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.of(
                       ERROR_KEY,
                       exception.getMessage() == null
                       ? UNABLE_TO_PROCESS_ERROR
                       : exception.getMessage().split(System.lineSeparator())[0]
                   ))
                   .type(MediaType.APPLICATION_JSON)
                   .build();
  }
}
