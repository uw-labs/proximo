using System.Collections.Generic;
using System.Threading.Tasks;

namespace proximo_dotnet
{
    public interface IPublisherClient
    {
        /// <summary>
        /// Publish messages to proximo server and adds the confirmation ids to an in-memory queue
        /// </summary>
        /// <param name="messagesList">A list of string messages</param>
        /// <param name="receiveQueue">The in-memory queue.</param>
        Task PublishMessages((string, string) message);

        /// <summary>
        /// Publish messages to proximo server and adds the confirmation ids to an in-memory queue
        /// </summary>
        /// <param name="messagesList">A list of byte[] messages</param>
        /// <param name="receiveQueue">The in-memory queue.</param>
        Task<string> PublishMessages((string, byte[]) message);
    }
}