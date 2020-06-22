// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Polly.Fallback;
using System.Threading;
using Microsoft.Azure.WebJobs.Host.Executors;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Logging;

namespace Microsoft.Azure.WebJobs.Extensions.Polly
{
    /// <summary>
    /// The class that implements a function execution with retries.
    /// </summary>
    public class PollyRetryManager : IRetryManager
    {
        private readonly ILogger _logger;
        private readonly string _id;
        private RetryAttribute _initialRetryPolicy;
        private const string RetryCountString = "retryCount";
        private const string RetryAttribute = "retryAttribute";

        /// <summary>
        /// Constructs a new instance.
        /// </summary>
        /// <param name="retryAttribute">Attribute that can be applied at the class or function level to set executions retries of job functions.</param>
        /// <param name="loggerFactory">The <see cref="ILoggerFactory"/> to create an <see cref="ILogger"/> from.</param>
        public PollyRetryManager(RetryAttribute retryAttribute, ILoggerFactory loggerFactory)
        {
            _id = Guid.NewGuid().ToString();
            _logger = loggerFactory?.CreateLogger(LogCategories.Executor);
            _initialRetryPolicy = retryAttribute;
        }

        /// <summary>
        /// Executes a functoin with retries.
        /// </summary>
        /// <param name="executeFunc">The function execution action to perform.</param>
        /// <param name="input">Input value for a triggered function execution.</param>
        /// <returns>Returns functions execution result.</returns>
        public async Task<FunctionResult> ExecuteWithRetriesAsync(Func<TriggeredFunctionData, CancellationToken, Task<FunctionResult>> executeFunc, TriggeredFunctionData input, CancellationToken token)
        {
            return await ExecuteWithRetriesInternalAsync(executeFunc, input, token, _initialRetryPolicy);
        }

        private async Task<FunctionResult> ExecuteWithRetriesInternalAsync(Func<TriggeredFunctionData, CancellationToken, Task<FunctionResult>> executeFunc, TriggeredFunctionData input, CancellationToken token, RetryAttribute retryAttribute)
        {
            FunctionResult functionResult = null;
            try
            {
                if (retryAttribute != null && retryAttribute.RetryCount != 0)
                {
                    AsyncFallbackPolicy fallbackPolicy = Policy
                           .Handle<FunctionInvocationException>()
                           .FallbackAsync((cancellationToken) =>
                           {
                               // Adding empty fallback to prevent throwing an exception after all retries have been exhausted 
                               Log("All retries have been exhausted");
                               return Task.CompletedTask;
                           });
                    Context context = new Context { { RetryCountString, 0 }, { PollyRetryManager.RetryAttribute, retryAttribute } };
                    await fallbackPolicy
                        .WrapAsync(GetRetryPolicy(retryAttribute))
                        .ExecuteAsync(async (ctx, tkn) =>
                        {
                            int retryCount = (int)ctx[RetryCountString];
                            SetRetry(input, retryCount);
                            functionResult = await executeFunc(input, token);
                            if (functionResult != null && functionResult.Exception != null)
                            {
                                throw functionResult.Exception;
                            }
                        }, context, token);
                }
                else
                {
                    SetRetry(input, 0);
                    functionResult = await executeFunc(input, token);
                    if (functionResult.Exception is RetryException)
                    {
                        throw functionResult.Exception;
                    }
                }
            }
            catch (RetryException retryException)
            {
                Log($"Function code returned retry settings: '{retryException.RetryResult.Format()}'");
                await ExecuteWithRetriesInternalAsync(executeFunc, input, token, retryException.RetryResult);
            }
            catch (OperationCanceledException)
            {
                Log("Retries were canceled");
            }
            return functionResult;
        }

        private void SetRetry(TriggeredFunctionData input, int retryCount)
        {
            if (input.TriggerDetails == null)
            {
                input.TriggerDetails = new Dictionary<string, string>()
                {
                    { RetryCountString, retryCount.ToString() }
                };
            } 
            else
            {
                input.TriggerDetails[RetryCountString] = retryCount.ToString();
            }
        }

        private AsyncRetryPolicy GetRetryPolicy(RetryAttribute retryAttribute)
        {
            PolicyBuilder builder = Policy.Handle<FunctionInvocationException>();
            if (retryAttribute.RetryCount == -1)
            {
                return builder.WaitAndRetryForeverAsync(retryAttribute.ExponentialBackoff ? (Func<int, Context, TimeSpan>)GetDurationExponential 
                    : GetDuration, OnRetryExponentialAsync);
            }
            else
            {
                return builder.WaitAndRetryAsync(retryAttribute.RetryCount, retryAttribute.ExponentialBackoff ? (Func<int, Context, TimeSpan>)GetDurationExponential 
                    : GetDuration, OnRetryAsync);
            }
        }

        private TimeSpan GetDuration(int retryCount, Context context)
        {
            return ((RetryAttribute)context[RetryAttribute]).SleepDuration;
        }

        private TimeSpan GetDurationExponential(int retryCount, Context context)
        {
            return TimeSpan.FromSeconds(Math.Pow(2, retryCount));
        }

        private Task OnRetryAsync(Exception exception, TimeSpan timeSpan, int retryCount, Context context)
        {
            Log("New linear retry attempt", exception, retryCount, timeSpan, context);
            context[RetryCountString] = retryCount;
            return Task.CompletedTask;
        }

        private Task OnRetryExponentialAsync(Exception exception, int retryCount, TimeSpan timeSpan, Context context)
        {
            Log("New exponential retry attempt", exception, retryCount, timeSpan, context);
            context[RetryCountString] = retryCount;
            return Task.CompletedTask;
        }

        private void Log(string message)
        {
            _logger.LogDebug($"Id: '{_id}', Message: '{message}'");
        }

        private void Log(string message, Exception ex, int retryCount, TimeSpan timeSpan, Context context)
        {
            RetryAttribute retryAttribute = context[RetryAttribute] as RetryAttribute;

            _logger.LogDebug($"Id: '{_id}', Message: '{message}', Exception: '{ex.ToString()}', Timestamp: '{timeSpan}', CurrentRetryCount: '{retryCount}'" +
                $"RetryCount: '{retryAttribute.RetryCount}', SleepDuration: '{retryAttribute.SleepDuration}', ExponentialBackoff: '{retryAttribute.ExponentialBackoff}'");
        }
    }
}
