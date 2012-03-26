<?php
/**
 * @see http://www.tomkleinpeter.com/2008/03/17/programmers-toolbox-part-3-consistent-hashing/
 */
class ConsistentHashedCache {

	private static $connections = array(); // map ip to connection
	private static $ids = array(); // map id/hash to ip

	public static function get($key) {
		$id = self::getCacheKey($key);
		$conn = self::$connections[self::findServer($id)];
		return "{$conn}->get($id)";
	}

	/**
	 * Write value to cache
	 * @param string $key
	 * @param mixed $value
	 * @return boolean indicating successful write to all servers
	 */
	public function set($key, $value) {
		$ok = true;
		$id = self::getCacheKey($key);
		foreach (self::findServers($id) as $server) {
			$conn = self::$connections[$server];
			if (!$conn->set($id)) {
				$ok = false;
			}
		}
		return $ok;

	}

	public static function init(array $servers) {
		foreach ($servers as $ip => $weight) {
			self::addServer($ip, $weight);
		}
		ksort(self::$ids);
		print_r(self::$ids);
	}

	private static function addServer($ip,$weight) {
		$conn = $ip; // establish the memcached connection
		if (!$conn) {
			return false;
		}
		self::$connections[$ip] = $conn;
		for (; $weight; $weight--) {
			self::$ids[self::getCacheKey("$ip-$weight")] = $ip;
		}
	}

	/**
	 * Find the server on which resource at key $id resides
	 * @param int $id
	 * @return string
	 */
	// This should be as fast as possible since it's run on every read+write
	private static function findServer($id) {
		$found = false;
		foreach (self::$ids as $serverId => $ip) {
			if ($serverId >= $id) {
				$found = true;
				break;
			}
		}
		reset(self::$ids); // Always reset pointer
		// If we didn't find a server, loop around and grab the first one off the list
		if (!$found) {
			$ip = current(self::$ids);
		}
		return $ip;
	}

	private static function findServers($id) {
		return array(self::findServer($id));
	}

	// This should be as fast as possible since it's run on every read+write
	private static function getCacheKey($str) {
		// The article says to use a 64-bit int. PHP doesn't do unsigned 64-bit ints properly, so we're just going with 60 bits instead (drop the leading hex)
		return hexdec(substr(md5($str), -15));
		// PHP doesn't really support unsigned 64-bit int, so we're going to mask off the high bit and make 63-bit numbers. Meh.
		// return hexdec(substr(md5($str), -16)) & ~ 0x8000000000000000;
	}

}


$servers = array
( '10.0.0.1:11211' => 20
, '10.0.0.2:11211' => 20
, '10.0.0.3:11211' => 10
);


ConsistentHashedCache::init($servers);
var_dump(ConsistentHashedCache::get('10.0.0.2:11211-2'));
var_dump(ConsistentHashedCache::get('foo'));
