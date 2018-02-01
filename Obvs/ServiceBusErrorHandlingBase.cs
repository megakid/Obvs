using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Obvs.Extensions;

namespace Obvs
{
    public abstract class ServiceBusErrorHandlingBase<TMessage, TCommand, TEvent, TRequest, TResponse> : IDisposable
        where TMessage : class
        where TCommand : TMessage
        where TEvent : TMessage
        where TRequest : TMessage
        where TResponse : TMessage
    {
        protected readonly Subject<Exception> _exceptions;

        protected ServiceBusErrorHandlingBase()
        {
            _exceptions = new Subject<Exception>();
        }

        public IObservable<Exception> Exceptions => _exceptions;

        protected static string EventErrorMessage(IEndpoint<TMessage> endpoint)
        {
            return $"Error publishing event to endpoint {endpoint.GetType().FullName}";
        }

        protected static string ReplyErrorMessage(IEndpoint<TMessage> endpoint)
        {
            return $"Error sending response to endpoint {endpoint.GetType().FullName}";
        }

        protected static string CommandErrorMessage(IEndpoint<TMessage> endpoint)
        {
            return $"Error sending command to endpoint {endpoint.GetType().FullName}";
        }

        protected static string EventErrorMessage(TEvent ev)
        {
            return $"Error publishing event {ev}";
        }

        protected static string ReplyErrorMessage(TRequest request, TResponse response)
        {
            return $"Error replying to request {request} with response {response}";
        }

        protected static string CommandErrorMessage(TCommand command)
        {
            return $"Error sending command {command}";
        }

        protected static string CommandErrorMessage()
        {
            return "Error sending commands";
        }

        protected void Catch(Action action, List<Exception> exceptions, string message = null)
        {
            try
            {
                action();
            }
            catch (Exception exception)
            {
                exceptions.Add(message == null ? exception : new Exception(message, exception));
            }
        }

        protected async Task CatchAsync(Func<Task> func, List<Exception> exceptions, string message = null)
        {
            try
            {
                await func().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                exceptions.Add(message == null ? exception : new Exception(message, exception));
            }
        }

        public virtual void Dispose()
        {
            _exceptions.Dispose();
        }
    }
}