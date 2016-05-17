"use strict";

var common = require('./runner'),
    stream = require('stream'),
    util = require('util'),
    Duplex = stream.Duplex,
    PassThrough = stream.PassThrough;

function RightDuplex(left)
{
    Duplex.call(this);
    this.left = left;
}

util.inherits(RightDuplex, Duplex);

RightDuplex.prototype._read = function ()
{
    if (this._cb)
    {
        this._cb();
        this._cb = null;
    }
};

RightDuplex.prototype._write = function (chunk, encoding, cb)
{
    if (this.left.push(chunk, encoding))
    {
        cb();
    }
    else
    {
        this._cb = cb;
    }
};

function LeftDuplex()
{
    Duplex.call(this);
    this.right = new RightDuplex(this);
}

util.inherits(LeftDuplex, Duplex);

LeftDuplex.prototype._read = function ()
{
    if (this._cb)
    {
        this._cb();
        this._cb = null;
    }
};

LeftDuplex.prototype._write = function (chunk, encoding, cb)
{
    if (this.right.push(chunk, encoding))
    {
        cb();
    }
    else
    {
        this._cb = cb;
    }
};

function connect(cb)
{
    cb(new LeftDuplex());
}

function accept(left, cb)
{
    cb(left.right);
}

common('in-memory', connect, accept);
