using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Obvs.Configuration;
using Obvs.Extensions;
using Obvs.Types;

namespace Obvs
{
    public interface IServiceBus : IServiceBusClient, IServiceBus<IMessage, ICommand, IEvent, IRequest, IResponse>
    {
    }

    public interface IServiceBus<TMessage, TCommand, TEvent, TRequest, TResponse> : IServiceBusClient<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        IObservable<TRequest> Requests { get; }
        IObservable<TCommand> Commands { get; }

        Task PublishAsync(TEvent ev);
        Task ReplyAsync(TRequest request, TResponse response);
    }

    public class ServiceBus : IServiceBus, IDisposable
    {
        private readonly IServiceBus<IMessage, ICommand, IEvent, IRequest, IResponse> _serviceBus;

        public ServiceBus(IEnumerable<IServiceEndpointClient<IMessage, ICommand, IEvent, IRequest, IResponse>> endpointClients, 
            IEnumerable<IServiceEndpoint<IMessage, ICommand, IEvent, IRequest, IResponse>> endpoints, 
            IRequestCorrelationProvider<IRequest, IResponse> requestCorrelationProvider = null) :
            this(new ServiceBus<IMessage, ICommand, IEvent, IRequest, IResponse>(endpointClients, endpoints, requestCorrelationProvider ?? new DefaultRequestCorrelationProvider()))
        {
        }

        public ServiceBus(IServiceBus<IMessage, ICommand, IEvent, IRequest, IResponse> serviceBus)
        {
            _serviceBus = serviceBus;
        }

        public static ICanAddEndpoint<IMessage, ICommand, IEvent, IRequest, IResponse> Configure() 
            => new ServiceBusFluentCreator<IMessage, ICommand, IEvent, IRequest, IResponse>(new DefaultRequestCorrelationProvider());

        public IObservable<IEvent> Events 
            => _serviceBus.Events;

        public Task SendAsync(ICommand command) 
            => _serviceBus.SendAsync(command);

        public Task SendAsync(IEnumerable<ICommand> commands)
            => _serviceBus.SendAsync(commands);

        public IObservable<IResponse> GetResponses(IRequest request)
            => _serviceBus.GetResponses(request);

        public IObservable<T> GetResponses<T>(IRequest request) where T : IResponse 
            => _serviceBus.GetResponses<T>(request);

        public IObservable<T> GetResponse<T>(IRequest request) where T : IResponse 
            => _serviceBus.GetResponse<T>(request);

        public IObservable<Exception> Exceptions 
            => _serviceBus.Exceptions;

        public IDisposable Subscribe(object subscriber, IScheduler scheduler = null) 
            => _serviceBus.Subscribe(subscriber, scheduler);

        public IObservable<IRequest> Requests 
            => _serviceBus.Requests;

        public IObservable<ICommand> Commands 
            => _serviceBus.Commands;

        public Task PublishAsync(IEvent ev) 
            => _serviceBus.PublishAsync(ev);

        public Task ReplyAsync(IRequest request, IResponse response)
            => _serviceBus.ReplyAsync(request, response);

        public void Dispose() 
            => ((IDisposable)_serviceBus).Dispose();
    }

    public class ServiceBus<TMessage, TCommand, TEvent, TRequest, TResponse> : 
        ServiceBusClient<TMessage, TCommand, TEvent, TRequest, TResponse>, IServiceBus<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        private readonly IRequestCorrelationProvider<TRequest, TResponse> _requestCorrelationProvider;

        public ServiceBus(IEnumerable<IServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse>> endpointClients, 
                          IEnumerable<IServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>> endpoints, 
                          IRequestCorrelationProvider<TRequest, TResponse> requestCorrelationProvider, 
                          IMessageBus<TMessage> localBus = null, LocalBusOptions localBusOption = LocalBusOptions.MessagesWithNoEndpointClients)
            : base(endpointClients, endpoints, requestCorrelationProvider, localBus, localBusOption)
        {
            _requestCorrelationProvider = requestCorrelationProvider;

            Requests = Endpoints
                .Select(endpoint => endpoint.RequestsWithErrorHandling(_exceptions)).Merge()
                .Merge(GetLocalMessages<TRequest>())
                .PublishRefCountRetriable();

            Commands = Endpoints
                .Select(endpoint => endpoint.CommandsWithErrorHandling(_exceptions)).Merge()
                .Merge(GetLocalMessages<TCommand>())
                .PublishRefCountRetriable();
        }

        public IObservable<TRequest> Requests { get; }

        public IObservable<TCommand> Commands { get; }

        public async Task PublishAsync(TEvent ev)
        {
            List<Exception> exceptions = new List<Exception>();

            var tasks = EndpointsThatCanHandle(ev)
                .Select(endpoint => CatchAsync(() => endpoint.PublishAsync(ev), exceptions, EventErrorMessage(endpoint)))
                .Concat(PublishLocal(ev, exceptions))
                .ToArray();


            if (tasks.Length == 0)
            {
                throw new Exception(
                    $"No endpoint or local bus configured for {ev}, please check your ServiceBus configuration.");
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            if (exceptions.Any())
            {
                throw new AggregateException(EventErrorMessage(ev), exceptions);
            }
        }

        public async Task ReplyAsync(TRequest request, TResponse response)
        {
            if (_requestCorrelationProvider == null)
            {
                throw new InvalidOperationException("Please configure the ServiceBus with a IRequestCorrelationProvider using the fluent configuration extension method .CorrelatesRequestWith()");
            }

            _requestCorrelationProvider.SetCorrelationIds(request, response);

            List<Exception> exceptions = new List<Exception>();

            var tasks = EndpointsThatCanHandle(response)
                    .Select(endpoint => CatchAsync(() => endpoint.ReplyAsync(request, response), exceptions, ReplyErrorMessage(endpoint)))
                    .Concat(PublishLocal(response, exceptions))
                    .ToArray();


            await Task.WhenAll(tasks).ConfigureAwait(false);

            if (exceptions.Any())
            {
                throw new AggregateException(ReplyErrorMessage(request, response), exceptions);
            }
        }

        public static ICanAddEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> Configure()
        {
            return new ServiceBusFluentCreator<TMessage, TCommand, TEvent, TRequest, TResponse>();
        }

        private IEnumerable<IServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>> EndpointsThatCanHandle(TMessage message)
        {
            return Endpoints.Where(endpoint => endpoint.CanHandle(message)).ToArray();
        }

        public override void Dispose()
        {
            base.Dispose();
            foreach (IServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> endpoint in Endpoints)
            {
                endpoint.Dispose();
            }
        }

        public override IDisposable Subscribe(object subscriber, IScheduler scheduler = null)
        {
            void OnReply(TRequest request, TResponse response) => ReplyAsync(request, response);

            IObservable<TMessage> messages = (Commands as IObservable<TMessage>).Merge(Events);

            return Subscribe(subscriber, messages, scheduler, Requests, OnReply);
        }
    }
}