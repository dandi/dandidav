Introduction to the WebDAV Protocol
===================================

[WebDAV](https://webdav.org) (specified in [RFC 4918][], obsoleting [RFC
2518][]) is a set of extensions to HTTP that enables using a network to access,
inspect, & traverse hierarchies of files & directories — or, in WebDAV terms,
non-collection resources and collection resources.

Requests for information about a hierarchy are implemented as HTTP requests
with the WebDAV-specific `PROPFIND` verb, and a successful response uses the
WebDAV-specific status code 207 ("Multi-Status"), which allows for reporting in
a single response the statuses of individual property requests on individual
resources.  See below for more information.

`GET` requests for a non-collection resource are responded to with the content
of that resource.

`GET` requests for a collection resource are not handled by WebDAV; the server
simply returns whatever it would have returned anyway.  To quote RFC 4918:

> GET when applied to a collection may return the contents of an "index.html"
> resource, a human-readable view of the contents of the collection, or
> something else altogether.  Hence it is possible that the result of a GET on
> a collection will bear no correlation to the membership of the collection.

In other words, the human-friendly tabular listings of directory entries seen
when viewing a `dandidav` page in a non-WebDAV-aware browser are a convenience
for humans' sake and not part of the actual WebDAV implementation.

WebDAV also defines functionality for creating, uploading, deleting, and
locking resources.  This is not implemented by `dandidav` and is not discussed
here.

`PROPFIND` Requests
-------------------

In order for a client to discover information about a resource and, optionally,
also its child resources, it must make a `PROPFIND` request to the resource's
URL.  Aside from the request path, the `PROPFIND` request is parameterized by
an optional "Depth" header and an optional "propfind" XML body.

### `Depth` Header

When requesting information on a collection resource, the `Depth` header
indicates which child resources of the collection to return information on as
well.  The possible values are:

- `0` — Only return information about the specified collection

- `1` — Only return information about the specified collection and its
  immediate children

- `infinity` — Return information about the specified collection and all of its
  descendant resources.  This is the default if no `Depth` header is specified.

A server may choose to not support the `infinity` option for performance
reasons.  If it does so, then infinite-depth requests should be rejected with a
403 status and a response body of:

```xml
<?xml version="1.0" encoding="utf-8"?>
<error xmlns="DAV:">
    <propfind-finite-depth />
</error>
```

### Request Body

The body of a `PROPFIND` request, when nonempty, must be an XML document
containing elements in the "`DAV:`" namespace (but see "XML Extensibility"
below).  The root element is named "propfind", and its contents must be one of
the following:

1. An empty "propname" element, indicating a request for a list of all property
   names (sans values) defined on the resource(s)

   Example request:

    ```xml
    <?xml version="1.0" encoding="utf-8"?>
    <propfind xmlns="DAV:">
        <propname />
    </propfind>
    ```

2. An empty "allprop" element, indicating a request for the names & values of
   (almost) all properties defined on the resource(s).  Specifically, this
   requests all "live properties" (those enforced or calculated by the server,
   like `getcontentlength`) that are defined by the RFC, along with all
   available "dead properties" (those freely settable by clients).  The
   "allprop" element may optionally be followed by an "include" element
   containing empty property elements of additional live properties to query.

   Example request:

    ```xml
    <?xml version="1.0" encoding="utf-8"?>
    <propfind xmlns="DAV:">
        <allprop />
        <include>
            <!-- These properties are defined by RFC 3253: -->
            <supported-live-property-set />
            <supported-report-set />
        </include>
    </propfind>
    ```

3. A "prop" element containing empty property elements of specific properties
   to query on the resource(s).

   Example request:

    ```xml
    <?xml version="1.0" encoding="utf-8"?>
    <propfind xmlns="DAV:">
        <prop>
            <getcontentlength />
            <resourcetype />
        </prop>
    </propfind>
    ```

An empty request body is equivalent to "allprop" without an "include".

### `PROPFIND` Responses

A successful or partially-successful `PROPFIND` response uses status code 207
("Multi-Status") and has a request body consisting of an XML document
containing elements in the "`DAV:`" namespace (but see "XML Extensibility"
below) whose root element is named "multistatus".

The DTD for "multistatus" and related elements is as follows:

```xml
<!ELEMENT multistatus (response*, responsedescription?)>
<!ELEMENT response (href, ((href*, status)|(propstat+)),
                    error?, responsedescription?, location?)>
<!ELEMENT href (#PCDATA)>
<!ELEMENT propstat (prop, status, error?, responsedescription?)>
<!ELEMENT prop ANY>  <!-- contains property elements -->
<!ELEMENT status (#PCDATA)>
<!ELEMENT error ANY>  <!-- contains precondition or postcondition codes -->
<!ELEMENT responsedescription (#PCDATA)>
<!ELEMENT location (href)>
```

In detail:

- The "multistatus" element contains zero or more "response" elements, each one
  containing the requested information for a single resource.  For a `PROPFIND`
  request, there will always be a "response" for the resource that the request
  was made to.  If that resource is a collection resource and the "Depth"
  header was not "0", there will be also be a "response" for each child or
  descendant resource.  The responses are organized as a flat list for which
  order is not significant.

- A "responsedescription" element contains text to optionally be displayed to
  the user.

- An "href" element inside a "response" contains the URL of the resource that
  the response describes, as either a complete URL or as the absolute path
  component of a URL.  In the latter case, the path must begin with a forward
  slash and include all path components "leading up to" the resource from the
  server root, and the complete URL is calculated by resolving this path
  against the URL of the request (roughly, by combining the non-path portions
  of the request URL with the path).  All "href" elements values within a
  Multi-Status response must be unique and must use the same choice of URL or
  absolute path.

- The "propstat" elements inside a "response" contain the names and possibly
  also values of the resource's properties, along with a status indicating the
  success of the request for the information.  Each "propstat" groups together
  properties that share the same status (though the RFC does not seem to
  require that the groups be maximal).

    - A "prop" element contains one or more property elements (see "Properties"
      below).  If the request was not for "propname" and the server was able to
      successfully determine the values of the given properties, the elements
      will contain the corresponding properties' values; otherwise, the
      elements will be empty.

    - A "status" element contains an HTTP status line (e.g., "200 OK" or "404
      NOT FOUND") describing the server's success in obtaining the properties
      in the neighboring "prop" element.  The status code may be any code in
      the 2xx, 3xx, 4xx, or 5xx range (though it is unclear when a 3xx code
      would be used).

- It appears that the `(href*, status)` production listed as an alternative to
  "propstat" in the DTD is only used for responses to verbs other than
  `PROPFIND`; the RFC isn't 100% clear.

- The "location" element is seemingly only used in conjunction with 3xx
  "status" elements immediately inside a response, and thus they presumably do
  not apply to `PROPFIND` responses; the RFC isn't clear.

- "error" elements contain precondition or postcondition codes identifying a
  violated requirement.  None of the codes defined in RFC 4918 would appear in
  an "error" inside a Multi-Status response to a `PROPFIND` request.

See [RFC 4918, §9.1.3 *et sequentes*][examples] for some examples of `PROPFIND`
response bodies.


The `Dav` Header
----------------

A response to an `OPTIONS` request to a WebDAV-enabled URL must include a "Dav"
header whose value is a comma-separated list of tokens identifying the
"compliance classes" that the resource supports.  Class "1" is for resources
that meet all "MUST" requirements in the RFC.  Class "2" is for resources that
support class "1" and also various locking-related functionality.  Class "3" is
for resources that support class "1" and also the changes made to RFC 2518 by
RFC 4918.

Properties
----------

In `PROPFIND` requests & responses, DAV resource properties are represented by
XML elements of the same name in the "`DAV:`" namespace (but see "XML
Extensibility" below), with the contents of the elements (when present) giving
the properties' values.

The properties defined by the RFC that do not involve locks are as follows.
Unless otherwise specified, the contents of the corresponding XML elements are
`#PCDATA`.  Properties that mirror HTTP response headers use the values as
would be returned for a `GET` request without any `Accept` headers.

- `creationdate` — timestamp of when the resource was created, in RFC 3339
  format

- `displayname` — name of the resource to display to the user

- `getcontentlanguage` — `Content-Language` HTTP header value for the resource

- `getcontentlength` — `Content-Length` HTTP header value for the resource

- `getcontenttype` — `Content-Type` HTTP header value for the resource

- `getetag` — `Etag` HTTP header value for the resource

- `getlastmodified` — `Last-Modified` HTTP header value for the resource

    - Note that the RFC states that the value of the `getlastmodified` property
      must be in rfc1123-date format as defined by RFC 2616, §3.3.1, which, in
      `strftime` format, is `%a, %d %b %Y %H:%M:%S GMT`, e.g., "Mon, 08 Jul
      2024 19:05:32 GMT".

- `resourcetype` — Specifies the nature of the resource.  In XML, the
  "resourcetype" element contains zero or more child elements, each of which is
  an identifier for a resource type that the resource belongs to.  The only
  resource type defined by the RFC is "collection".  Thus, if a "resourcetype"
  element contains a "collection" element, the resource is a collection, and if
  it does not contain a "collection" element, the resource is a non-collection.


XML Extensibility
-----------------

TODO


[RFC 2518]: http://webdav.org/specs/rfc2518.html
[RFC 4918]: http://webdav.org/specs/rfc4918.html
[examples]: http://webdav.org/specs/rfc4918.html#rfc.section.9.1.3
