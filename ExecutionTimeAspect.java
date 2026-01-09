package com.bns.fsl.emt.incoming.event.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Aspect
@Component
@Slf4j
public class ExecutionTimeAspect {

    @Around("@annotation(com.bns.fsl.emt.incoming.event.aspect.LogExecutionTime)")
    public Object logExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.nanoTime();
        
        // 1. Context Extraction
        String requestId = determineContextId(joinPoint);
        String methodName = joinPoint.getSignature().getName();

        // 2. Log START (Clean)
        log.info("method_execution_start method={} requestId={}", methodName, requestId);

        try {
            // 3. Execute Method
            Object proceed = joinPoint.proceed();
            
            // 4. Log END (Clean)
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            log.info("method_execution_end method={} requestId={} durationMs={}", methodName, requestId, durationMs);
            
            return proceed;

        } catch (Throwable e) {
            // 5. Log FAILURE (Clean)
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            log.error("method_execution_fail method={} requestId={} durationMs={} error='{}'", 
                      methodName, requestId, durationMs, e.getMessage());
            throw e; 
        }
    }

    private String determineContextId(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();

        // Strategy A: Explicit "requestId" parameter
        if (paramNames != null) {
            for (int i = 0; i < paramNames.length; i++) {
                if ("requestId".equals(paramNames[i]) && args[i] instanceof String) {
                    return (String) args[i];
                }
            }
        }

        // Strategy B: Kafka Record Key
        for (Object arg : args) {
            if (arg instanceof ConsumerRecord<?, ?> record) {
                return "Key-" + record.key();
            }
        }
        return "N/A";
    }
}
