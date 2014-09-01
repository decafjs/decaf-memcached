/**
 * Created with JetBrains WebStorm.
 * User: mschwartz
 * Date: 7/24/13
 * Time: 7:48 AM
 * To change this template use File | Settings | File Templates.
 */

/*global sync, builtin */

"use strict";

/**
 * @private
 */
var {AddrUtil,MemcachedClient} = Packages.net.spy.memcached,
    Thread = require('Threads').Thread;

// Connection Pooling
var pool = [];

var getConnection = sync(function(addresses) {
    return pool.pop() || new MemcachedClient(AddrUtil.getAddresses(addresses));
}, pool);

var releaseConnection = sync(function(con) {
    pool.push(con);
}, pool);

var cleanup = sync(function() {
    while (true) {
        var conn = pool.pop();
        if (!conn) {
            return;
        }
        conn.shutdown();
    }
});

builtin.atExit(function() {
    console.log('cleanup memcached');
    cleanup();
});

/**
 * Construct a Memcached connection
 *
 * @param {string} addresses whitespace or comma separated host oor IP addresses and port numbers in the form host:port
 * @constructor
 */
function Memcached(addresses) {
    var me = this,
        t = Thread.currentThread();

    addresses = addresses || '127.0.0.1:11211';
    this.client = t.memcached || getConnection(addresses);
    t.memcached = this.client;
    t.on('endRequest', function() {
        delete t.memcached;
        releaseConnection(me.client);
    });
}
decaf.extend(Memcached.prototype, {
    /**
     * Set an object in the cache regardless of any existing value.
     *
     * The expires value is passed along to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * > Note that the return will be false any time a mutation has not occurred.
     * >
     * > The actual value sent may either be Unix time (number of seconds since January 1, 1970, as a 32-bit value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time.
     *
     * @param {string} key name to add
     * @param {string} o value to add
     * @param {int} expires optional expiration value.  If not provided, 0 will be used.
     * @chainable
     */
    set: function(key, o, expires) {
        this.client.set(key, expires||0, o);
        return this;
    },

    /**
     * Add an object to the cache if it does not exist already.
     *
     * The expires value is passed along to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * > Note that the return will be false any time a mutation has not occurred.
     * >
     * > The actual value sent may either be Unix time (number of seconds since January 1, 1970, as a 32-bit value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time.
     *
     * @param {string} key name to add
     * @param {string} o value to add
     * @param {int} expires optional expiration value.  If not provided, 0 will be used.
     * @chainable
     */
    add: function(key, o, expires) {
        this.client.add(key, expires||0, o);
        return this;
    },
    /**
     * Replace an object with the given value (transcoded with the default transcoder) if there is already a value for the given key.
     *
     * The expires value is passed along to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * > Note that the return will be false any time a mutation has not occurred.
     * >
     * > The actual value sent may either be Unix time (number of seconds since January 1, 1970, as a 32-bit value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time.
     *
     * @param {string} key name to add
     * @param {string} o value to add
     * @param {int} expires optional expiration value.  If not provided, 0 will be used.
     * @chainable
     */
    replace: function(key, o, expires) {
        this.client.replace(key, expires||0, o);
        return this;
    },

    /**
     * Prepend to an existing value in the cache.
     *
     * @param {string} key key name to whose value will be prepended
     * @param {string} value value to prepend
     * @chainable
     */
    prepend: function(key, value) {
        this.client.prepend(key, value);
        return this;
    },

    /**
     * Append to an existing value in the cache.
     *
     * @param {string} key name to append to
     * @param {string} value value to append
     * @chainable
     */
    append: function(key, value) {
        this.client.append(key, value);
        return this;
    },

    /**
     * Get object from the database.
     *
     * @param key name of value to get
     * @returns {string} value
     */
    get: function(key) {
        return String(this.client.get(key));
    },

    /**
     * Get values for multiple keys from the database.
     *
     * @param {array} keys array of strings, the keys
     * @returns {array} values
     */
    mget: function(keys) {
        return this.client.getBulk(keys);
    },

    /**
     * Delete the given key from the cache.
     *
     * @param {string} key name of key to remove
     * @chainable
     */
    remove: function(key) {
        this.client['delete'](key);
        return this;
    },

    /**
     * Flush all caches from all servers immediately.
     *
     * @chainable
     */
    flush: function() {
        this.client.flush();
        return this;
    },
    close: function() {
        releaseConnection(this.client);
    }
});

decaf.extend(exports, {
    Memcached: Memcached
});
