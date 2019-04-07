package com.projects.bigdata.cassandra_microservice.error

import org.springframework.http.ResponseEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.context.request.WebRequest
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestController
import java.util.Date


@ControllerAdvice
@RestController
class ErrorController : ResponseEntityExceptionHandler() {

    @ExceptionHandler(Throwable::class)
    fun handleAllException(t: Throwable, request: WebRequest): ResponseEntity<RequestErrorWrapper> {
        val headers = HttpHeaders()
        headers.contentType = MediaType.APPLICATION_JSON
        return ResponseEntity(RequestErrorWrapper(Date(), t.message!!, request.getDescription(false)), headers, HttpStatus.INTERNAL_SERVER_ERROR)
    }
}