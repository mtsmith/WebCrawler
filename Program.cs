using HtmlAgilityPack;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Channels
{
    public enum TagType
    {
        Anchor = 1,
        Image = 2,
        IFrame = 3,
        Link = 4,
        Video = 5,
        Audio = 6,
    }

    public static class UriHelpers
    {
        public static Uri Combine (this Uri baseUri, string url)
        {
            // baseUri=http://abc.com/path1, url=http://xyz.com/path2 => http://xyz.com/path2
            if (url.StartsWith ("http"))
            {
                return new Uri (url);
            }

            // baseUri=http://abc.com/path1, url=//abc.com/path2 => http://abc.com/path2
            if (url.StartsWith ("//"))
            {
                return new Uri (string.Format ("{0}:{1}", baseUri.Scheme, url));
            }

            // baseUri=http://abc.com/path1, url=/path2 => http://abc.com/path2
            if (url.StartsWith ("/"))
            {
                var urlBuilder = new UriBuilder (baseUri);
                urlBuilder.Path = url;
                return urlBuilder.Uri;
            }
            else
            {
                // baseUri=http://abc.com/path1, url=path2 => http://abc.com/path1/path2
                var urlBuilder = new UriBuilder (baseUri);
                urlBuilder.Path += string.Format ("/{0}", url);
                return urlBuilder.Uri;
            }
        }
    }

    public class Tag : IEquatable<Tag>
    {
        public Tag (Uri parentUri, string childUrl, TagType tagType)
        {
            ParentUri = parentUri;
            ChildUrl = childUrl;
            TagType = tagType;
            IsSameSite = true;
            IsUrlValid = true;

            try
            {
                if (ChildUrl.StartsWith ("http"))
                {
                    var uri = new Uri (ChildUrl);
                    IsSameSite = uri.Authority == parentUri.Authority;
                }
                else
                {
                    var uri = new Uri (ParentUri, childUrl);
                }
            }
            catch (Exception ex)
            {
                IsUrlValid = false;
                ErrorMessage = ex.Message;
            }
        }

        public string Url
        {
            get
            {
                return ParentUri.Combine (ChildUrl).AbsoluteUri;
            }
        }
        public Uri ParentUri { get; }
        public string ChildUrl { get; }
        public TagType TagType { get; }
        public bool IsSameSite { get; }
        public bool IsUrlValid { get; }
        public string ErrorMessage { get; }

        public string ToString ()
        {
            return Url.ToString ();
        }

        public override bool Equals (object obj)
        {
            return Equals (obj as Tag);
        }

        public bool Equals (Tag other)
        {
            return other != null
                && ParentUri == other.ParentUri
                && ChildUrl == other.ChildUrl;
        }

        public override int GetHashCode ()
        {
            return HashCode.Combine (ParentUri, ChildUrl);
        }
    }

    public static class TagHelpers
    {
        public static string ToFilePath (this Tag tag)
        {
            var uri = new Uri (tag.Url);
            string host = uri.Host.Replace (@"\", "");
            string path = uri.AbsolutePath.Replace (@"/", @"\");
            // Fix special case for web root "/" => "_taskId.html"
            if (path == @"\")
            {
                path += "_taskId.html";
            }
            string filePath = string.Format (@".\{0}{1}", host, path);
            return filePath;
        }
    }

    public class UrlCache
    {
        private readonly HashSet<Tag> Cache = new HashSet<Tag> ();

        public bool Add (Tag tag)
        {
            return Cache.Add (tag);
        }

        public bool Contains (Tag tag)
        {
            return Cache.Contains (tag);
        }
    }

    public class WorkQueue
    {
        ConcurrentQueue<Tag> Queue = new ConcurrentQueue<Tag> ();

        public bool IsEmpty => Queue.IsEmpty;

        public void Enqueue (Tag tag)
        {
            Queue.Enqueue (tag);
        }

        public bool Dequeue (out Tag tag)
        {
            return Queue.TryDequeue (out tag);
        }
    }

    public class ErrorList : Dictionary<string, string>
    {
        public new void Add (string key, string value)
        {
            base.Add (key, value);
        }
    }

    public class WebCrawler
    {
        UrlCache UrlCache = new UrlCache ();
        WorkQueue WorkQueue = new WorkQueue ();
        int ConcurrencyLevel;

        public WebCrawler (string url, bool sameSiteOnly, int concurrencyLevel = 3)
        {
            BaseUri = new UriBuilder (url).Uri;
            Blacklist = new HashSet<string> ();
            ErrorList = new ErrorList ();
            ConcurrencyLevel = concurrencyLevel;
            SameSiteOnly = sameSiteOnly;
        }

        public Uri BaseUri { get; }
        public HashSet<string> Blacklist { get; set; }
        public ErrorList ErrorList { get; set; }
        public bool SameSiteOnly { get; set; }

        public async Task Crawl ()
        {
            var DownloadChannel = Channel.CreateBounded<Tag> (
                new BoundedChannelOptions (3)
                {
                    SingleWriter = true,
                    SingleReader = false,
                });

            // Add our root url to the queue
            var tag = new Tag (BaseUri, "/", TagType.Anchor);
            WorkQueue.Enqueue (tag);

            // Using a Semaphore to limit the number of concurrent tasks
            SemaphoreSlim downloadSemaphore = new SemaphoreSlim (ConcurrencyLevel, ConcurrencyLevel);

            List<Task> downloadTasks = CreateSubscriber (DownloadChannel, downloadSemaphore);
            await CreatePublisher (DownloadChannel, downloadSemaphore);

            await Task.WhenAll (downloadTasks);
        }

        private List<Task> CreateSubscriber (Channel<Tag> DownloadChannel, SemaphoreSlim downloadSemaphore)
        {
            var downloadTasks = new List<Task> ();
            for (var i = 0; i < ConcurrencyLevel; i++)
            {
                int taskId = i;
                downloadTasks.Add (Task.Factory.StartNew (async () =>
                {
                    while (await DownloadChannel.Reader.WaitToReadAsync ())
                    {
                        if (DownloadChannel.Reader.TryRead (out Tag tag))
                        {
                            Trace.WriteLine (string.Format ("{0}: Download [{1}] {2}", taskId, tag.TagType.ToString (), tag.Url));

                            try
                            {
                                string filePath = tag.ToFilePath ();
                                string directoryName = Path.GetDirectoryName (filePath);
                                if (!Directory.Exists (directoryName))
                                {
                                    Directory.CreateDirectory (directoryName);
                                }

                                using (var client = new HttpClient ())
                                {
                                    if (tag.TagType == TagType.Anchor)
                                    {
                                        await DownloadDocumentAsync (taskId, tag, client, filePath);
                                    }
                                    else
                                    {
                                        await DownloadFileAsync (taskId, tag, client, filePath);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Trace.WriteLine (string.Format ("{0}: Error: {1}", taskId, ex.Message));
                                ErrorList.Add (tag.ChildUrl, ex.Message);
                            }
                            finally
                            {
                                downloadSemaphore.Release ();
                            }
                        }
                    }

                    Trace.WriteLine (string.Format ("{0}: Download Channel Complete", taskId));

                }, TaskCreationOptions.LongRunning));
            }

            return downloadTasks;
        }

        private async Task CreatePublisher (Channel<Tag> DownloadChannel, SemaphoreSlim downloadSemaphore)
        {
            await Task.Run (async () =>
            {
                while (true)
                {
                    if (WorkQueue.Dequeue (out Tag work))
                    {
                        Trace.WriteLine (string.Format ("Write [{0}] {1}", work.TagType.ToString (), work.Url));

                        await downloadSemaphore.WaitAsync ();
                        await DownloadChannel.Writer.WriteAsync (work);
                    }
                    else
                    {
                        if (downloadSemaphore.CurrentCount == ConcurrencyLevel)
                        {
                            Trace.WriteLine ("Queue Empty");
                            DownloadChannel.Writer.TryComplete ();
                            return;
                        }
                    }
                }
            });
        }

        private async Task DownloadDocumentAsync (int taskId, Tag tag, HttpClient client, string filePath)
        {
            Trace.WriteLine (string.Format ("{0}: Downloading {1} => {2}", taskId, tag.ChildUrl, filePath));

            var content = await client.GetStringAsync (tag.Url);
            HtmlDocument doc = LoadDocument (content);

            using (var fileStream = File.Create (filePath))
            {
                doc.Save (fileStream);
            }

            EnqueueUrls (GetAnchors (doc), BaseUri, TagType.Anchor);
            EnqueueUrls (GetImages (doc), BaseUri, TagType.Image);
            EnqueueUrls (GetLinks (doc), BaseUri, TagType.Link);
        }

        private void EnqueueUrls (IEnumerable<string> childUrls, Uri parentUri, TagType tagType)
        {
            foreach (var childUrl in childUrls)
            {
                try
                {
                    if (Blacklist.Contains (childUrl))
                    {
                        continue;
                    }

                    var tag = new Tag (parentUri, childUrl, tagType);

                    if (!UrlCache.Add (tag))
                    {
                        continue;
                    }

                    if (!tag.IsUrlValid)
                    {
                        continue;
                    }

                    if (SameSiteOnly && !tag.IsSameSite)
                    {
                        continue;
                    }

                    Trace.WriteLine (string.Format ("Enqueue [{0}] {1}", tag.TagType.ToString (), tag.Url));

                    WorkQueue.Enqueue (tag);
                }
                catch (Exception ex)
                {
                    ErrorList.Add (childUrl, ex.Message);
                }
            }
        }

        private static async Task DownloadFileAsync (int taskId, Tag tag, HttpClient client, string filePath)
        {
            client.MaxResponseContentBufferSize = int.MaxValue; // Go big or go home

            Trace.WriteLine (string.Format ("{0}: Downloading {1} => {2}", taskId, tag.ChildUrl, filePath));

            using (var inputStream = await client.GetStreamAsync (tag.Url))
            {
                using (var fileStream = File.Create (filePath))
                {
                    await inputStream.CopyToAsync (fileStream);
                }
            }
        }

        private static IEnumerable<string> GetLinks (HtmlDocument doc)
        {
            var nodes = doc.DocumentNode.SelectNodes ("//link[@rel='stylesheet']");
            if (nodes != null)
            {
                foreach (var node in nodes)
                {
                    // This should only ever return a single value
                    foreach (var href in node.Attributes.AttributesWithName ("href"))
                    {
                        yield return href.Value;
                    }
                }
            }
        }

        private static IEnumerable<string> GetImages (HtmlDocument doc)
        {
            var nodes = doc.DocumentNode.SelectNodes ("//img");
            if (nodes != null)
            {
                foreach (var node in nodes)
                {
                    // This should only ever return a single value
                    foreach (var src in node.Attributes.AttributesWithName ("src"))
                    {
                        yield return src.Value;
                    }
                }
            }
        }

        private static IEnumerable<string> GetAnchors (HtmlDocument doc)
        {
            var nodes = doc.DocumentNode.SelectNodes ("//a");
            if (nodes != null)
            {
                foreach (var node in nodes)
                {
                    // This should only ever return a single value
                    foreach (var href in node.Attributes.AttributesWithName ("href"))
                    {
                        yield return href.Value;
                    }
                }
            }
        }

        private static HtmlDocument LoadDocument (string content)
        {
            HtmlDocument doc = new HtmlDocument ();
            doc.LoadHtml (content);
            return doc;
        }
    }

    class Program
    {
        static async Task Main (string[] args)
        {
            var webCrawler = new WebCrawler (args[0], sameSiteOnly: true);
            webCrawler.Blacklist.Add ("/#");
            await webCrawler.Crawl ();
        }
    }
}
