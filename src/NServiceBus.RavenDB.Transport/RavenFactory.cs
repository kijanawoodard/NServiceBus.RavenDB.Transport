using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;

namespace NServiceBus.Features
{
    class RavenFactory
    {
        private readonly string _defaultConnectionString;
        private readonly IDictionary<string, string> _extendedConnectectionStrings;
        private readonly string _endpointName;
        private readonly IDictionary<string, IDocumentStore> _stores;
        private readonly IDictionary<string, IDocumentSession> _sessions;
 
        public RavenFactory(string defaultConnectionString, IDictionary<string, string> extendedConnectectionStrings, string endpointName)
        {
            _defaultConnectionString = defaultConnectionString;
            _extendedConnectectionStrings = extendedConnectectionStrings;
            _endpointName = endpointName;

            _stores = new Dictionary<string, IDocumentStore>();
            _sessions = new Dictionary<string, IDocumentSession>();

            var unique = new[] {defaultConnectionString}.Union(_extendedConnectectionStrings.Values);
            foreach (var connectionString in unique)
            {
                var store = new DocumentStore();
                store.ParseConnectionString(connectionString);
                store.Initialize();

                _stores[connectionString] = store;    
            }
        }

        public IDocumentStore FindDocumentStore(string queue)
        {
            string key;
            var found = _extendedConnectectionStrings.TryGetValue(queue, out key);
            if (!found) key = _defaultConnectionString; //assume same document store as endpoint if not specified in config; convience. Too much magic?

            return _stores[key];
        }

        public IDocumentSession OpenSession()
        {
            return _stores[_defaultConnectionString].OpenSession(_endpointName);
        }

        /*public IDocumentSession OpenSession(string messageId)
        {
            IDocumentSession session;
            var found = _sessions.TryGetValue(messageId, out session);

            if (!found || session == null)
            {
                session = _stores[_defaultConnectionString].OpenSession(_endpointName);
                _sessions[messageId] = session;
            }

            return session;
        }

        public void UsingSession(string messageId, Action<IDocumentSession> action)
        {
            IDocumentSession session;
            var found = _sessions.TryGetValue(messageId, out session);

            if (found)         //we're inside a using
            {
                action(session);
                return;
            }

            using (session = _stores[_defaultConnectionString].OpenSession(_endpointName))
            {
                action(session);
            }
        }*/

        public IDocumentSession OpenRemoteSession(string queue)
        {
            return FindDocumentStore(queue).OpenSession(queue);
        }
    }
}