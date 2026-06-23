/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.pyryze.infrastructure.ratelimit;

import com.pyryze.infrastructure.ratelimit.RateLimiter.PreExecute;

/**
 * Exception thrown when a task execution is denied because a rate limit
 * has been exceeded.
 *
 * <p>The associated {@link PreExecute} contains information about the
 * rate limit that caused the rejection.</p>
 *
 * @author Onyeche Joshua Blessing
 */
public class RateLimitException extends RuntimeException {
  
  private static final long serialVersionUID = 4L;
  private PreExecute preExecute = null;
  
  /**
   * Creates a new {@code RateLimitException}.
   *
   * @param msg the exception message
   */
  public RateLimitException(String msg) {
    super(msg);
  }
  
  /**
   * Sets the associated rate limit state that caused this exception.
   *
   * @param preExecute the associated rate limit state
   */
  public void setPreExecute(PreExecute preExecute){
      this.preExecute = preExecute;
  }
  
  /**
   * Gets the associated rate limit state that caused this exception.
   *
   * @return the associated rate limit state, or {@code null} if not set
   */
  public PreExecute getPreExecute(){
      return preExecute;
  }
}