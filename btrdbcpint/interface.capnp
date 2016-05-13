# Interface to interact with BTrDB, written by Michael Andersen.

using Go = import "go.capnp";
$Go.package("cpinterface");
$Go.import("github.com/SoftwareDefinedBuildings/btrdb/cpinterface");

@0x85360901bcc4bed2;

###
# Request type, each request gives back exactly one response
###
struct Request {
    # This will be added to the response, so that requests can be mapped
    # to responses as they can come back out of order.
    echoTag     @0 : UInt64;
    union {
        void                    @1 : Void;
        queryStandardValues     @2 : CmdQueryStandardValues;
        queryStatisticalValues  @3 : CmdQueryStatisticalValues;
        queryWindowValues       @9 : CmdQueryWindowValues;
        queryVersion            @4 : CmdQueryVersion;
        queryNearestValue       @5 : CmdQueryNearestValue;
        queryChangedRanges      @6 : CmdQueryChangedRanges;
        insertValues            @7 : CmdInsertValues;
        deleteValues            @8 : CmdDeleteValues;
    }
}

# The basic record type. Times are measured in nanoseconds
# since the Epoch. At the time of writing, BTrDB is only
# capable of storing dates from approx 1935 to 2078...
struct Record {
    time    @0 : Int64;
    value   @1 : Float64;
}

# Query pre-aggregated statistical records from the database.
# these are particularly useful for plotting applications
# and locating where data is.
struct StatisticalRecord {
    time        @0 : Int64;
    count       @1 : UInt64;
    min         @2 : Float64;
    mean        @3 : Float64;
    max         @4 : Float64;
}

# Query from startTime (inclusive) to endTime (exclusive) in
# nanoseconds.
# If you want consistent values over a series of
# reads, or you wish to view a stream as it was in the past
# then you can specify a nonzero version. Repeating a query
# with the same version is guaranteed to return the same results
# irrespective of any deletes or adds that take place.
# returns many RecordLists
struct CmdQueryStandardValues {
    uuid        @0 : Data;
    version     @1 : UInt64;
    startTime   @2 : Int64;
    endTime     @3 : Int64;
}


# Query from startTime (inclusive) to endTime (exclusive) in
# nanoseconds. Note that both of those times will be rounded
# down if they have set bits in the bottom pointWidth bits.
# pointWidth is the log of the number of records to aggregate
# per result. A PW of 30 therefore means (1<<30) ns per record
# which is about a second.
# If you want consistent values over a series of
# reads, or you wish to view a stream as it was in the past
# then you can specify a nonzero version
# returns many StatisticalRecordLists
struct CmdQueryStatisticalValues {
    uuid        @0 : Data;
    version     @1 : UInt64;
    startTime   @2 : Int64;
    endTime     @3 : Int64;
    pointWidth  @4 : UInt8;
}

# Query from startTime (inclusive) to endTime (exclusive) in
# nanoseconds. Aggregate windows with an end time less than or equal
# to endTime will be returned. Windows start from exactly startTime and
# increase by Width. Leap seconds etc are your problem. The depth
# (currently unimplemented) represents the minimum PW to descend to
# while computing windows.
# If you want consistent values over a series of
# reads, or you wish to view a stream as it was in the past
# then you can specify a nonzero version
# returns many StatisticalRecordLists
struct CmdQueryWindowValues {
    uuid        @0 : Data;
    version     @1 : UInt64;
    startTime   @2 : Int64;
    endTime     @3 : Int64;
    width       @4 : UInt64;
    depth       @5 : UInt8;
}

# For every UUID given, return the current version and last
# modified time of the stream.
# returns VersionList
struct CmdQueryVersion {
    uuids       @0 : List(Data);
}

# Query the next (or previous if backward=true) value in the
# stream, starting from time.
# returns a RecordList
struct CmdQueryNearestValue {
    uuid        @0 : Data;
    version		  @1 : UInt64;
    time        @2 : Int64;
    backward    @3 : Bool;
}

# For the given UUID, return all the time ranges that have
# changed between the given generations. toGeneration is
# not included. Note that depending on how full the stream is,
# the returned result may be rounded off. A sparsely populated
# stream returns less accurate results than a densely populated
# one.
# returns many RangeLists
struct CmdQueryChangedRanges {
    uuid            @0 : Data;
    fromGeneration  @1 : UInt64;
    toGeneration    @2 : UInt64;
    unused          @3 : UInt64;
    resolution      @4 : UInt8;
}

# Insert values. If sync is true, the database will flush the
# results to disk before returning success. Please PLEASE don't
# use that without seriously considering if you need it, as it
# disables transaction coalescence and reduces performance
# by several orders of magnitude.
# returns Void
struct CmdInsertValues {
    uuid        @0 : Data;
    values      @1 : List(Record);
    sync        @2 : Bool;
}

# Delete the values between the given times.
# returns Void
struct CmdDeleteValues {
    uuid        @0 : Data;
    startTime   @1 : Int64;
    endTime     @2 : Int64;
}

###
# Response type
###
struct Response {
    echoTag                     @0 : UInt64;
    statusCode                  @1 : StatusCode;
    final                       @2 : Bool;
    union {
        void                    @3 : Void;
        records                 @4 : Records;
        statisticalRecords   	  @5 : StatisticalRecords;
        versionList             @6 : Versions;
        changedRngList          @7 : Ranges;
    }
}

# Contains all the error codes that are emitted by Quasar
enum StatusCode {
    ok                      @0;

    # Returned (ATM) for almost everything
    internalError           @1;

    # Returned for a bad UUID or a bad version
    noSuchStreamOrVersion   @2;

    # Returned for a bad parameter, like time range
    invalidParameter        @3;

    # Returned from nearest value when it doesn't exist
    noSuchPoint				@4;
}

# Contains a list of records, and the version of the stream
# used to satisfy the request.
struct Records {
    version  @0 : UInt64;
    values   @1 : List(Record);
}

# Contains a list of statistical records and the version of
# the stream used to satisfy the request.
struct StatisticalRecords {
    version @0 : UInt64;
    values  @1 : List(StatisticalRecord);
}

# Contains the latest version numbers for the requested
# streams
struct Versions {
    uuids       @0 : List(Data);
    versions    @1 : List(UInt64);
}

# Represents a range of time that has been changed
struct ChangedRange {
    startTime       @0 : Int64;
    endTime         @1 : Int64;
}

# Response to the QueryChangedRanges
struct Ranges {
    version			@0 : UInt64;
    values      @1 : List(ChangedRange);
}
