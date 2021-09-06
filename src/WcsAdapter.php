<?php

namespace Jncinet\Flysystem\Wcs;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\CanOverwriteFiles;
use Wcs\Http\PutPolicy;
use Wcs\MgrAuth;
use Wcs\SrcManage\FileManager;
use Wcs\Upload\StreamUploader;
use Wcs\Upload\Uploader;
use Wcs\Utils;

class WcsAdapter extends AbstractAdapter implements CanOverwriteFiles
{
    /**
     * @var string
     */
    protected $bucket;

    /**
     * WcsAdapter constructor.
     *
     * @param string $bucket
     */
    public function __construct(string $bucket)
    {
        $this->bucket = $bucket;
    }

    /**
     * Get the S3Client bucket.
     *
     * @return string
     */
    public function getBucket()
    {
        return $this->bucket;
    }

    /**
     * Set the S3Client bucket.
     *
     * @param string $bucket
     * @return void
     */
    public function setBucket(string $bucket)
    {
        $this->bucket = $bucket;
    }

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param array $config
     *
     * @return false|array false on failure file meta data on success
     */
    public function write($path, $contents, $config = [])
    {
        $pp = new PutPolicy();
        $pp->overwrite = \Wcs\Config::WCS_OVERWRITE;
        $pp->scope = $this->bucket . ':' . $path;
        $token = $pp->get_token();

        $ramPath = $config['ram_path'] ?? __DIR__ . DIRECTORY_SEPARATOR;

        // 创建临时文件
        $filepath = $ramPath . basename($path);
        $file = fopen($filepath, 'w') or die("Unable to open file!");
        fwrite($file, $contents);
        fclose($file);

        $client = new Uploader($token);
        $rs = $client->upload_return($path);

        // 删除临时文件
        unlink($filepath);
        return $rs;
    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param array $config Config object
     *
     * @return false|array false on failure file meta data on success
     */
    public function update($path, $contents, $config = [])
    {
        return $this->write($path, $contents, $config);
    }

    /**
     * @param string $path
     * @param string $contents
     * @param array $config
     * @return array|false
     */
    public function put(string $path, string $contents, array $config = [])
    {
        return $this->write($path, $contents, $config);
    }

    /**
     * @param string $path
     * @param resource $contents
     * @param array $config
     * @return array|false
     */
    public function putStream(string $path, $contents, array $config = [])
    {
        return $this->writeStream($path, $contents, $config);
    }

    /**
     * Rename a file.
     *
     * @param string $from
     * @param string $to
     * @return bool
     * @throws \Exception
     */
    public function rename($from, $to)
    {
        if (!$this->copy($from, $to)) {
            return false;
        }

        return $this->delete($from);
    }

    /**
     * Delete a file.
     *
     * @param string $path
     * @return bool
     * @throws \Exception
     */
    public function delete($path)
    {
        $ak = \Wcs\Config::WCS_ACCESS_KEY;
        $sk = \Wcs\Config::WCS_SECRET_KEY;

        $auth = new MgrAuth($ak, $sk);

        $client = new FileManager($auth);

        $client->delete($this->bucket, $path);

        return !$this->has($path);
    }

    /**
     * @param $path
     * @return array|false
     * @throws \Exception
     */
    public function readAndDelete($path)
    {
        $content = $this->read($path);
        if ($content !== false) {
            $this->delete($path);
            return $content;
        } else {
            return false;
        }
    }

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     */
    public function deleteDir($dirname)
    {
        return false;
    }

    /**
     * Create a directory.
     *
     * @param string $dirname
     * @return bool
     */
    public function createDir($dirname, $config = [])
    {
        return false;
    }

    /**
     * Check whether a file exists.
     *
     * @param string $path
     * @return bool
     * @throws \Exception
     */
    public function has($path)
    {
        return $this->read($path) !== false;
    }

    /**
     * Read a file.
     *
     * @param string $path
     * @return array|false
     * @throws \Exception
     */
    public function read($path)
    {
        $response = Utils::http_get($path, null);
        if ($response->code == 200) {
            return $response->respBody;
        }
        return false;
    }

    /**
     * List contents of a directory.
     *
     * @param string $directory
     * @param bool $recursive
     *
     * @return array
     */
    public function listContents($directory = '', $recursive = false)
    {
        $ak = \Wcs\Config::WCS_ACCESS_KEY;
        $sk = \Wcs\Config::WCS_SECRET_KEY;

        $auth = new MgrAuth($ak, $sk);

        $client = new FileManager($auth);
        $res = $client->bucketList($this->bucket);

        if ($res->code == 200) {
            return $res->respBody;
        }
        return [];
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return false|array
     */
    public function getMetadata($path)
    {

        $ak = \Wcs\Config::WCS_ACCESS_KEY;
        $sk = \Wcs\Config::WCS_SECRET_KEY;

        $auth = new MgrAuth($ak, $sk);
        $client = new FileManager($auth);
        $res = $client->stat($this->bucket, $path);
        if ($res->code == 200) {
            return $res->respBody;
        }
        return false;
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return false|integer
     */
    public function getSize($path)
    {
        $data = $this->getMetadata($path);
        if ($data) {
            return $data['fsize'];
        }
        return false;
    }

    /**
     * Get the mimetype of a file.
     *
     * @param string $path
     *
     * @return false|string
     */
    public function getMimetype($path)
    {
        $data = $this->getMetadata($path);
        if ($data) {
            return $data['mimeType'];
        }
        return false;
    }

    /**
     * Get the timestamp of a file.
     *
     * @param string $path
     *
     * @return false|integer
     */
    public function getTimestamp($path)
    {
        $data = $this->getMetadata($path);
        if ($data) {
            return $data['putTime'];
        }
        return false;
    }

    /**
     * Write a new file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param array $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function writeStream($path, $resource, $config = [])
    {
        $pp = new PutPolicy();
        $pp->overwrite = \Wcs\Config::WCS_OVERWRITE;
        if ($path == null || $path == '') {
            $pp->scope = $this->bucket;
        } else {
            $pp->scope = $this->bucket . ':' . $path;
        }
        $token = $pp->get_token();

        $client = new StreamUploader($token);
        $res = $client->upload_return($resource);
        if ($res->code == 200) {
            return $res->respBody;
        }
        return false;
    }

    /**
     * Update a file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param array $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, $config = [])
    {
        return $this->writeStream($path, $resource, $config);
    }

    /**
     * Copy a file.
     *
     * @param string $from
     * @param string $to
     *
     * @return bool
     */
    public function copy($from, $to)
    {
        $ak = \Wcs\Config::WCS_ACCESS_KEY;
        $sk = \Wcs\Config::WCS_SECRET_KEY;
        $auth = new MgrAuth($ak, $sk);
        $client = new FileManager($auth);
        $res = $client->copy($this->bucket, $from, $this->bucket, $to);
        return $res == 200;
    }

    /**
     * Read a file as a stream.
     *
     * @param string $path
     * @return array|false
     * @throws \Exception
     */
    public function readStream($path)
    {
        return $this->read($path);
    }

    /**
     * Set the visibility for a file.
     *
     * @param string $path
     * @param string $visibility
     * @return array
     */
    public function setVisibility($path, $visibility)
    {
        return compact('path', 'visibility');
    }

    /**
     * Get the visibility of a file.
     *
     * @param string $path
     *
     * @return array
     */
    public function getVisibility($path)
    {
        return ['visibility' => AdapterInterface::VISIBILITY_PUBLIC];
    }
}
