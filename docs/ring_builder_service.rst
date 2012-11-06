
-----------------------
Ring Builder Middleware
-----------------------

The ring_builder middleware is available for use on internal proxies
to provide a api for creating, obtaining, and modifying the ring.

A sample proxy-server.conf configuration for a ring builder server would
look as follows::

    [DEFAULT]
    bind_port = 8080
    user = swift
    swift_dir = /etc/swift
    log_facility = LOG_LOCAL1

    [pipeline:main]
    pipeline = ring_builder proxy-server

    [app:proxy-server]
    use = egg:swift#proxy

    [filter:ring_builder]
    use = egg:rbm#rbm
    key = myringpasskey
    # directory where to create backups
    backup_dir = /etc/swift/backups
    #names of the builder files:
    #account_builder = account.builder
    #container_builder = container.builder
    #object_builder = object.builder
    #names of the ring files:
    #account_ring = account.ring.gz
    #container_ring = container.ring.gz
    #object_ring = object.ring.gz

The above configuration would allow you to access the ring builder api on port
8080. Backups would be created in /etc/swift/backups as the ring and builder
files are modified. The rings and builder files would be created in /etc/swift.
This configuration will expose the following API endpoints:

==================================  ========================================
Request URI                         Description
----------------------------------  ----------------------------------------
POST /ringbuilder/<type>/add        Add a list of devices to the ring
POST /ringbuilder/<type>/remove     Remove a list of devices from the ring
POST /ringbuilder/<type>/rebalance  Rebalance the ring
POST /ringbuilder/<type>/weight     Change the weight of devices
POST /ringbuilder/<type>/meta       Change the meta info of devices
POST /ringbuilder/<type>/search     Search for devices in the ring
HEAD /ringbuilder/<type>.builder    Obtain the md5sum of a builder file
GET /ringbuilder/<type>.builder     Download a builder file
GET /ringbuilder/<type>/list        Get a list of ALL devices in the builder
HEAD /ring/<type>.tar.gz            Get md5sum of a ring.gz
GET /ring/<type>.tar.gz             Download a ring.gz
==================================  ========================================


In all cases <type> is either 'account', 'container', or 'object' depending on
which ring builder or ring file you wish to operate on.

To interact with the API the 'X-RING-BUILDER-KEY' request header must be set.
In any method that modifies the ring the 'X-RING-BUILDER-LAST-HASH' header must
also be set. The X-RING-BUILDER-LAST-HASH header is used to ensure that the on disk
builder files are in the state expected and haven't been modified or altered.
As a safety precaution most of these operations also lock the builder files.
This is to ensure that multiple conccurent requests do not alter the builders
in unexpected ways. (Such as would be the case if someone deletes a device
while someone else is in the middle of a rebalance or the like).

A note about the rebalance call. The rebalance end point is only included for
completeness. A ring rebalance in production can take a significant amount of time
to perform (minutes) and is best managed outside of this scope since its usually
accompanied by a ring deployment anyway.

A basic walk through using curl::

    Download a ring file:

        $ curl -H "X-RING-BUILDER-KEY: yourpasskey" -X GET \
          http://localhost:8080/ring/object.ring.gz -v -o test.file
        > GET /ring/object.ring.gz HTTP/1.1
        > User-Agent: curl/7.19.7 (x86_64-pc-linux-gnu) libcurl/7.19.7 OpenSSL/0.9.8k zlib/1.2.3.3 libidn/1.15
        > Host: localhost:8080
        > Accept: */*
        > X-RING-BUILDER-KEY: yourpasskey
        > 
        < HTTP/1.1 200 OK
        < Content-Length: 254364
        < X-Current-Hash: f418d1ae8823e59eeba378484769ddcd
        < Content-Type: application/octet-stream
        < X-Trans-Id: tx04b9e26626474990aeccec8a3fa6c92a
        < Date: Mon, 02 Jul 2012 19:49:10 GMT
        < 
         [data not shown]

    Download a builder:

        $ curl -H "X-RING-BUILDER-KEY: yourpasskey" -X GET \
          http://localhost:8080/ringbuilder/account.builder -v -o test.file

    Perform a head to obtain current md5sum of builder or ring:

        $ curl -H "X-RING-BUILDER-KEY: yourpasskey" -X HEAD \
          http://localhost:8080/ringbuilder/object.builder -v
        > HEAD /ringbuilder/object HTTP/1.1
        > User-Agent: curl/7.19.7 (x86_64-pc-linux-gnu) libcurl/7.19.7 OpenSSL/0.9.8k zlib/1.2.3.3 libidn/1.15
        > Host: localhost:8080
        > Accept: */*
        > X-RING-BUILDER-KEY: yourpasskey
        > 
        < HTTP/1.1 200 OK
        < X-Current-Hash: 01346f415d429cb45cc74b15c5d88643
        < X-Trans-Id: txc57a610d028242d49f196e50e45e7e74
        < Content-Length: 0
        < Date: Mon, 02 Jul 2012 19:52:27 GMT
        < 

    The md5sum is returned in the X-Current-Hash header. We can now use it to
    manipulate the ring. For example to change the weight of a device in the object
    builder we'll post to /ringbuilder/object/weight:

        $ curl -v -H "X-RING-BUILDER-LAST-HASH: 01346f415d429cb45cc74b15c5d88643" \
          -H "X-RING-BUILDER-KEY: yourpasskey" -H "Content-Type: application/json" \
          -X POST -d '{"devices": {"1": "5.0", "2": "5.0"}}' \
          http://127.0.0.1:8080/ringbuilder/object/weight
        > POST /ringbuilder/object/weight HTTP/1.1
        > User-Agent: curl/7.19.7 (x86_64-pc-linux-gnu) libcurl/7.19.7 OpenSSL/0.9.8k zlib/1.2.3.3 libidn/1.15
        > Host: 127.0.0.1:8080
        > Accept: */*
        > X-RING-BUILDER-LAST-HASH: 01346f415d429cb45cc74b15c5d88643
        > X-RING-BUILDER-KEY: yourpasskey
        > Content-Type: application/json
        > Content-Length: 37
        > 
        < HTTP/1.1 200 OK
        < X-Current-Hash: 3ddb9854eb65cff458eedc6399c47841
        < X-Trans-Id: tx1cc5a256ff01473a932dce4ac48f82c1
        < Content-Length: 0
        < Date: Mon, 02 Jul 2012 20:02:07 GMT

    This changed the weight of the devices with device id 1 and 2 to 5.0. The
    response also includes the builders new md5sum in the X-Current-Hash header. A look
    at the proxy log files should also show lines indicating that the old builder
    was backed up and a new builder was written.

curl examples for the other end points and requirements are listed
below. In most cases the expected json post content mirrors the requirements of
swift-ring-builder. The end points are documented in the format of::

    METHOD /uri/endpoint - {The expected json content if any}
    ...
    a curl example and output

    A note on what errors may be returned.

POST /ringbuilder/<type>/weight - {"devices": {"$DEVID": "$NEW_WEIGHT"}}::

    curl -i -H "X-RING-BUILDER-KEY: yourpasskey" \
        -H "X-RING-BUILDER-LAST-HASH: 978dbc6af312c784853359fca17ae34a" \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -X POST -d '{"devices": {"1": "5.0", "2": "5.0"}}' \
        http://127.0.0.1:8080/ringbuilder/object/weight

    HTTP/1.1 200 OK
    X-Current-Hash: 9de1aabda53e811771811933a21b2c8a
    X-Trans-Id: tx1eac2efcc3a64a68a79194f32c3c06bb
    Content-Length: 0
    Date: Tue, 14 Aug 2012 07:35:55 GMT

    May return 400 Bad Request on malformed data or attempted modification of
    device thats not present. May return a 409 if the md5sum of the target
    differs or if the builder is already locked for an update.

POST /ringbuilder/<type>/meta - {"devices": {"$DEVID": "$NEW_VALUE"}}::

    curl -i -H "X-RING-BUILDER-KEY: yourpasskey" \
        -H "X-RING-BUILDER-LAST-HASH: 42afb7037a565235555e872644fe2a9c" \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -X POST -d '{"devices": {"1": "something", "2": "another"}}' \
        http://127.0.0.1:8080/ringbuilder/object/meta

    HTTP/1.1 200 OK
    X-Current-Hash: 2bd0d2b36f48afc371fa7e43d41fafef
    X-Trans-Id: tx2fdea52c176247ad975d3485ae95472a
    Content-Length: 0
    Date: Tue, 14 Aug 2012 07:39:52 GMT

    May return 400 Bad Request on malformed data or attempted modification of
    device thats not present. May return a 409 if the md5sum of the target
    differs or if the builder is already locked for an update.

POST /ringbuilder/<type>/remove - {"devices": ["$DEVID"]}::

    curl -i -H "X-RING-BUILDER-KEY: yourpasskey" \
        -H "X-RING-BUILDER-LAST-HASH: c85ea939b1173dc237d216c8e0214b48" \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -X POST -d '{"devices": ["1", "2"]}' \
        http://127.0.0.1:8080/ringbuilder/object/removie

    HTTP/1.1 200 OK
    X-Current-Hash: b44e256b8c97551b2044cc1054abf801
    X-Trans-Id: tx704ef1d4285c44f2bc3111b5a427d564
    Content-Length: 0
    Date: Tue, 14 Aug 2012 07:45:43 GMT

    May return 400 Bad Request on malformed data or attempted removal of device
    thats not present. May return a 409 if the md5sum of the target
    differs or if the builder is already locked for an update.

POST /ringbuilder/<type>/add::

    Sample json post contents:
    {"devices":
        [
         {"weight": 5.0, "zone": 1, "ip": "1.1.1.1", "meta": "a new dev", "device": "sda", "port": 6010},
         {"weight": 2.0, "zone": 1, "ip": "1.1.1.1", "meta": "another", "device": "sdb", "port": 6010}
        ]
    }

    curl -i -H "X-RING-BUILDER-KEY: yourpasskey" \
        -H "X-RING-BUILDER-LAST-HASH: b44e256b8c97551b2044cc1054abf801" \
        -H "Accept: application/json" -H "Content-Type: application/json" \
        -X POST -d '{"devices": [{"weight": 5.0, "zone": 1, "ip": "1.1.1.1", "meta": "a new dev", "device": "sda", "port": 6010}, {"weight": 2.0, "zone": 1, "ip": "1.1.1.1", "meta": "another", "device": "sdb", "port": 6010}]}' \
        http://127.0.0.1:8080/ringbuilder/object/add

    HTTP/1.1 200 OK
    X-Current-Hash: c43ccd3485878d42a0a9c9f098193cd9
    X-Trans-Id: txef5d2b5a49f94ef58ee4e89059381496
    Content-Length: 0
    Date: Tue, 14 Aug 2012 07:48:49 GMT

    May return 400 Bad Request on malformed data or on attempted addition of
    existing devices. May return a 409 if the md5sum of the target
    differs or if the builder is already locked for an update.

POST /ringbuilder/<type>/search - {"value": "$A_SEARCH_TERM"} - accepts the same
searches a swift-ring-builder search::

    curl -i -H "X-RING-BUILDER-KEY: yourpasskey" \
        -H "X-RING-BUILDER-LAST-HASH: 978dbc6af312c784853359fca17ae34a" \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        -X POST \
        -d '{"value": "d1"}' http://127.0.0.1:8080/ringbuilder/object/search

    HTTP/1.1 200 OK
    Content-Length: 136
    X-Current-Hash: 978dbc6af312c784853359fca17ae34a
    Content-Type: application/json
    X-Trans-Id: tx73f982a68b49439a9d122e0dae43fba0
    Date: Tue, 14 Aug 2012 07:26:56 GMT

    [
      {
        "weight": 1,
        "zone": 2,
        "parts": 196608,
        "port": 6020,
        "device": "sdb2",
        "id": 1,
        "meta": "",
        "ip": "127.0.0.1",
        "parts_wanted": 0
      }
    ]

    May return a 400 Bad Request on invalid search term. May return a 409 if
    the md5sum of the target differs or if the builder is already locked for an
    update.

POST /ringbuilder/<type>/rebalance - has no post body::

    Returns:
    {"reassigned": 4, "balance": 0, "partitions": 4}

    May generate a 400 Bad Request if the rebalance can't be performed because
    either no parts need to be assigned or none can be due to min_part_hours not
    having been met. May return a 409 if the md5sum of the target
    differs or if the builder is already locked for an update.

