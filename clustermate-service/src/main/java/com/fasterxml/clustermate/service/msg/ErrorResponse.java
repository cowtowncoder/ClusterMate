package com.fasterxml.clustermate.service.msg;

/**
 * Entity used for errors for operations other than CRUD.
 */
public class ErrorResponse
{
    public String message;

    public ErrorResponse(String message) {
        this.message = message;
    }
}
