// SPDX-License-Identifier: Unlicense
pragma solidity =0.8.15;

import {Math} from "@openzeppelin/contracts/utils/math/Math.sol";

interface AccessControl {
    // do not use `view` so that we allow some bookkeeping
    function canAppend(address) external returns (bool);
}

interface IonianStructs {
    // TODO: optimize this with tight variable packing
    struct LogEntry {
        uint256[] streamIds;
        bytes data;
        bytes32 dataRoot;
        uint256 sizeBytes;
    }

    struct Stream {
        AccessControl ac;
    }
}

interface IonianEvents {
    event NewStream(uint256 id);
}

interface IonianErrors {
    error TooManyStreams();

    error Unauthorized();
}

contract IonianLog is IonianStructs, IonianEvents, IonianErrors {
    // ------------------ constants ------------------

    uint256 public constant MAX_STREAMS_PER_LOG_ENTRY = 10;

    // ------------------ state variables ------------------

    LogEntry[] public log;
    Stream[] public streams;

    // ------------------ initialization ------------------

    constructor() {
        // reserve stream #0
        streams.push(Stream(AccessControl(address(0))));
    }

    // ------------------ log management ------------------

    function appendLog(bytes32 dataRoot, uint256 sizeBytes) external {
        log.push(LogEntry(new uint256[](0), bytes(""), dataRoot, sizeBytes));
    }

    function appendLog(
        bytes32 dataRoot,
        uint256 sizeBytes,
        uint256[] calldata streamIds
    ) external {
        log.push(LogEntry(streamIds, bytes(""), dataRoot, sizeBytes));
        checkStreams(streamIds);
    }

    function appendLogWithData(bytes calldata data) external {
        log.push(LogEntry(new uint256[](0), data, bytes32(0), 0));
    }

    function appendLogWithData(
        bytes calldata data,
        uint256[] calldata streamIds
    ) external {
        log.push(LogEntry(streamIds, data, bytes32(0), 0));
        checkStreams(streamIds);
    }

    // ------------------ stream management ------------------

    function createStream(AccessControl ac) external {
        uint256 id = streams.length;
        streams.push(Stream(ac));
        emit NewStream(id);
    }

    // ------------------ query interface ------------------

    function numLogEntries() external view returns (uint256) {
        return log.length;
    }

    function numStreams() external view returns (uint256) {
        return streams.length;
    }

    function getLogEntries(uint256 offset, uint256 limit)
    external
    view
    returns (LogEntry[] memory entries)
    {
        if (offset >= log.length) {
            return new LogEntry[](0);
        }

        uint256 endExclusive = Math.min(log.length, offset + limit);
        entries = new LogEntry[](endExclusive - offset);

        for (uint256 ii = offset; ii < endExclusive; ++ii) {
            entries[ii - offset] = log[ii];
        }
    }

    // ------------------ internal logic ------------------

    function checkStreams(uint256[] calldata streamIds) private {
        if (streamIds.length > MAX_STREAMS_PER_LOG_ENTRY) {
            revert TooManyStreams();
        }

        for (uint256 ii = 0; ii < streamIds.length; ii++) {
            uint256 streamId = streamIds[ii];
            Stream memory stream = streams[streamId];

            if (stream.ac != AccessControl(address(0))) {
                if (!stream.ac.canAppend(msg.sender)) {
                    revert Unauthorized();
                }
            }
        }
    }
}