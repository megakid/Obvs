using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Obvs.Types;

namespace Obvs.Tests
{
    public class FakeSubscriber2
    {
        public readonly List<IMessage> Received = new List<IMessage>();
        public bool ThrowExceptions { get; set; }

        public  async Task OnEvent(TestServiceEvent1 message)
        {
            Handle(message);
        }

        public  async Task OnEvent(TestServiceEvent2 message)
        {
            Handle(message);
        }

        public  async Task OnEvent(TestServiceEventBase message)
        {
            Handle(message);
        }

        public  async Task OnCommand(TestServiceCommand1 message)
        {
            Handle(message);
        }

        public  async Task OnCommand(TestServiceCommand2 message)
        {
            Handle(message);
        }

        public  async Task OnCommand(TestServiceCommandBase message)
        {
            Handle(message);
        }

        public IObservable<IResponse> OnRequest(TestServiceRequest1 request)
        {
            Handle(request);
            return Observable.Empty<IResponse>();
        }

        private void Handle(IMessage message)
        {
            if (ThrowExceptions)
            {
                throw new Exception("ThrowExceptions set to Equal");
            }
            Received.Add(message);
        }
    }
}