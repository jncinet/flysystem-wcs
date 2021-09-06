<?php

namespace Jncinet\Flysystem\Wcs\Sdk\Storage;

use Jncinet\Flysystem\Wcs\Sdk\Auth;
use Jncinet\Flysystem\Wcs\Sdk\Config;
use Jncinet\Flysystem\Wcs\Sdk\Support;

/**
 * 空间资源管理及批量操作接口
 */
final class BucketManager
{
    private $auth;
    private $config;

    public function __construct(Auth $auth, Config $config = null)
    {
        $this->auth = $auth;
        if ($config == null) {
            $this->config = new Config();
        } else {
            $this->config = $config;
        }
    }

    /**
     * 获取指定账号下所有的空间名
     *
     * @return array 包含所有空间名
     */
    public function buckets(): array
    {
        $list_buckets = $this->listbuckets();
        if (array_key_exists('code', $list_buckets) && $list_buckets['code'] == 200) {
            $names = [];
            foreach ($list_buckets['buckets'] as $key => $bucket) {
                $names[$key] = $bucket['name'];
            }
            return $names;
        }
        return $list_buckets;
    }

    /**
     * 列举空间，返回bucket列表
     *
     * @return array
     */
    public function listbuckets(): array
    {
        return $this->rsGet('/bucket/list');
    }

    /**
     * 获取空间存储量
     *
     * @param string $bucket 指定查询的空间列表，格式为：<bucket_name1>
     * @param string $startDate 统计开始时间，格式为yyyy-mm-dd
     * @param string $endDate 统计结束时间，格式为yyyy-mm-dd 注：查询的时间跨度最长为30天
     * @param bool $isListDetails 标识是否返回存储量明细
     * @param string|null $storageType 可选值为：Standard、InfrequentAccess、Archive分别代表标准、低频和归档的存储量，不填时会返回总存储量。
     * @return array
     */
    public function bucketInfo(string $bucket, string $startDate, string $endDate, bool $isListDetails = false, string $storageType = null)
    {
        return $this->bucketInfos($bucket, $startDate, $endDate, $isListDetails, $storageType);
    }

    /**
     * 获取空间存储量
     *
     * @param string $bucket 指定查询的空间列表，格式为：<bucket_name1>|<bucket_name2>|……
     * @param string $startDate 统计开始时间，格式为yyyy-mm-dd
     * @param string $endDate 统计结束时间，格式为yyyy-mm-dd 注：查询的时间跨度最长为30天
     * @param bool $isListDetails 标识是否返回存储量明细
     * @param string|null $storageType 可选值为：Standard、InfrequentAccess、Archive分别代表标准、低频和归档的存储量，不填时会返回总存储量。
     * @return array
     */
    public function bucketInfos(string $bucket, string $startDate, string $endDate, bool $isListDetails = false, string $storageType = null)
    {
        $qs = [
            'name' => $bucket,
            'startdate' => $startDate,
            'enddate' => $endDate,
            'isListDetails' => $isListDetails,
        ];
        if (!empty($storageType)) $qs['storageType'] = $storageType;
        $path = '/bucket/stat?' . http_build_query($qs);
        return $this->rsGet($path);
    }

    /**
     * 列取空间的文件列表
     *
     * @param string $bucket 空间名
     * @param string|null $prefix 列举前缀
     * @param string|null $marker 列举标识符
     * @param int $limit 单次列举个数限制
     * @param string|null $startTime 文件上传起始时间，格式为精确到毫秒的时间戳，如1526745600000
     * @param string|null $endTime 文件上传终止时间
     * @param int|null $mode
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/list
     */
    public function listFiles(
        string $bucket,
        string $prefix = null,
        string $marker = null,
        int    $limit = 1000,
        string $startTime = null,
        string $endTime = null,
        int    $mode = null
    ): array
    {
        $query = array('bucket' => $bucket);
        Support::setWithoutEmpty($query, 'prefix', $prefix);
        Support::setWithoutEmpty($query, 'marker', $marker);
        Support::setWithoutEmpty($query, 'limit', $limit);
        Support::setWithoutEmpty($query, 'startTime', $startTime);
        Support::setWithoutEmpty($query, 'endTime', $endTime);
        Support::setWithoutEmpty($query, 'mode', $mode);
        if (array_key_exists('prefix', $query)) {
            $query['prefix'] = Support::base64_urlSafeEncode($query['prefix']);
        }
        $url = $this->getRsfHost() . '/list?' . http_build_query($query);
        return $this->rsget($url);
    }

    /**
     * 获取资源的元信息，但不返回文件内容
     *
     * @param string $bucket 待获取信息资源所在的空间
     * @param string $key 待获取资源的文件名
     *
     * @return array
     * @link  https://wcs.chinanetcenter.com/document/API/ResourceManage/bucketstat
     */
    public function stat(string $bucket, string $key): array
    {
        $path = '/stat/' . Support::entry($bucket, $key);
        return $this->rsGet($path);
    }

    /**
     * 音视频元数据
     *
     * @param string $key 资源地址
     * @return array
     */
    public function statWithAv(string $key): array
    {
        return $this->rsGet($key . '?op=avinfo');
    }

    /**
     * 音视频简单元数据
     *
     * @param string $key
     * @return array
     */
    public function statWithAv2(string $key): array
    {
        return $this->rsGet($key . '?op=avinfo2');
    }

    /**
     * 删除指定资源
     *
     * @param string $bucket 待删除资源所在的空间
     * @param string $key 待删除资源的文件名
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/delete
     */
    public function delete(string $bucket, string $key): array
    {
        $path = '/delete/' . Support::entry($bucket, $key);
        return $this->rsPost($path);
    }

    public function deleteWithFops(string $fops, string $notifyURL = null, int $separate = 0)
    {
        return $this->fops('/fmgr/delete', $fops, $notifyURL, null, $separate);
    }

    public function deletePrefixWithFops(string $fops, string $notifyURL = null, int $separate = 0)
    {
        return $this->fops('/fmgr/deletePrefix', $fops, $notifyURL, null, $separate);
    }

    public function deleteM3u8WithFops(string $fops, string $notifyURL = null, int $separate = 0)
    {
        return $this->fops('/fmgr/deletem3u8', $fops, $notifyURL, null, $separate);
    }

    /**
     * 给资源进行重命名，本质为move操作。
     *
     * @param string $bucket 待操作资源所在空间
     * @param string $oldName 待操作资源文件名
     * @param string $newName 目标资源文件名
     *
     * @return array
     */
    public function rename(string $bucket, string $oldName, string $newName): array
    {
        return $this->move($bucket, $oldName, $bucket, $newName);
    }

    /**
     * 对资源进行复制。
     *
     * @param string $from_bucket 待操作资源所在空间
     * @param string $from_key 待操作资源文件名
     * @param string $to_bucket 目标资源空间名
     * @param string $to_key 目标资源文件名
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/copy
     */
    public function copy(string $from_bucket, string $from_key, string $to_bucket, string $to_key): array
    {
        $from = Support::entry($from_bucket, $from_key);
        $to = Support::entry($to_bucket, $to_key);
        $path = '/copy/' . $from . '/' . $to;
        return $this->rsPost($path);
    }

    /**
     * @param string $fops
     * @param string|null $notifyURL
     * @param int $force
     * @param int $separate
     * @return array|\Exception|\GuzzleHttp\Exception\RequestException|mixed
     */
    public function copyWithFops(string $fops, string $notifyURL = null, int $force = 1, int $separate = 0)
    {
        return $this->fops('/fmgr/copy', $fops, $notifyURL, $force, $separate);
    }

    /**
     * 将资源从一个空间到另一个空间
     *
     * @param string $from_bucket 待操作资源所在空间
     * @param string $from_key 待操作资源文件名
     * @param string $to_bucket 目标资源空间名
     * @param string $to_key 目标资源文件名
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/move
     */
    public function move(string $from_bucket, string $from_key, string $to_bucket, string $to_key): array
    {
        $from = Support::entry($from_bucket, $from_key);
        $to = Support::entry($to_bucket, $to_key);
        $path = '/move/' . $from . '/' . $to;
        return $this->rsPost($path);
    }

    public function moveWithFops(string $fops, string $notifyURL = null, int $force = 1, int $separate = 0)
    {
        return $this->fops('/fmgr/move', $fops, $notifyURL, $force, $separate);
    }

    /**
     * 更新镜像资源
     *
     * @param string $bucket
     * @param array $files
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/prefetch
     */
    public function prefetch(string $bucket, array $files): array
    {
        $path = '/prefetch/';
        foreach ($files as $key => $file) {
            $files[$key] = Support::base64_urlSafeEncode($file);
        }
        $fileKeys = implode('|', $files);
        $path .= Support::base64_urlSafeEncode($bucket . ':' . $fileKeys);
        return $this->rsPost($path);
    }

    /**
     * 从指定URL抓取资源，并将该资源存储到指定空间中
     *
     * @param string $fetchURL 指定抓取URL
     * @param string $bucket 指定存储空间
     * @param string|null $key
     * @param string|null $prefix
     * @param string|null $md5
     * @param string|null $decompression
     * @param int|null $crush
     * @param string|null $p
     * @param int|null $fetchTS
     * @param string|null $notifyURL 处理结果通知接收URL
     * @param int $force
     * @param int $separate
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/Fmgr/fetch
     */
    public function fetch(string $fetchURL,
                          string $bucket,
                          string $key = null,
                          string $prefix = null,
                          string $md5 = null,
                          string $decompression = null,
                          int    $crush = null,
                          string $p = null,
                          int    $fetchTS = null,
                          string $notifyURL = null,
                          int    $force = 0,
                          int    $separate = 0): array
    {
        $data = 'fops=fetchURL/' . Support::base64_urlSafeEncode($fetchURL);
        $data .= '/bucket/' . Support::base64_urlSafeEncode($bucket);
        if (!empty($key)) {
            $data .= '/key/' . Support::base64_urlSafeEncode($key);
        }
        if (!empty($prefix)) {
            $data .= '/prefix/' . Support::base64_urlSafeEncode($prefix);
        }
        if (!empty($md5)) {
            $data .= '/md5/' . $md5;
        }
        if (!empty($decompression)) {
            $data .= '/decompression/' . $decompression;
        }
        if (!empty($crush)) {
            $data .= '/crush/' . $crush;
        }
        if (!empty($p)) {
            $data .= '/p/' . $p;
        }
        if (!empty($fetchTS)) {
            $data .= '/fetchTS/' . $fetchTS;
        }
        if (!empty($notifyURL)) {
            $data .= '&notifyURL=' . Support::base64_urlSafeEncode($notifyURL);
        }
        if (!empty($force)) {
            $data .= '&force=' . $force;
        }
        if (!empty($separate)) {
            $data .= '&separate=' . $separate;
        }

        return $this->rspost('/fmgr/fetch', $data);
    }

    /**
     * 设置文件的生命周期
     *
     * @param string $bucket 设置文件生命周期文件所在的空间
     * @param string $key 设置文件生命周期文件的文件名
     * @param int $days 设置该文件多少天后删除，当$days设置为0表示尽快删除，-1表示取消过期时间，永久保存
     * @param int|null $relevance 操作m3u8文件时是否关联设置TS文件的保存期限。
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/setdeadline
     */
    public function deleteAfterDays(string $bucket, string $key, int $days, int $relevance = null): array
    {
        $data = 'bucket=' . Support::base64_urlSafeEncode($bucket);
        $data .= '&key=' . Support::base64_urlSafeEncode($key);
        $data .= '&deadline=' . $days;
        if (!is_null($relevance)) {
            $data .= '&relevance=' . $relevance;
        }
        return $this->rsPost('/setdeadline', $data);
    }

    public function deleteAfterDaysWithFops(string $fops, string $notifyURL = null, int $separate = 0)
    {
        return $this->fops('/fmgr/setdeadline', $fops, $notifyURL, null, $separate);
    }

    /**
     * 文件解压缩
     *
     * @param string $bucket
     * @param string $key
     * @param string $fops
     * @param string|null $notifyURL
     * @param int $force
     * @param int $separate
     *
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/decompression
     */
    public function unzip(string $bucket, string $key, string $fops, string $notifyURL = null, int $force = 0, int $separate = 0): array
    {
        $data = [
            'bucket' => Support::base64_urlSafeEncode($bucket),
            'key' => Support::base64_urlSafeEncode($key),
            'fops' => Support::base64_urlSafeEncode($fops),
        ];
        if (!empty($notifyURL)) {
            $data['notifyURL'] = Support::base64_urlSafeEncode($notifyURL);
        }
        $data['force'] = $force;
        $data['separate'] = $separate;
        return $this->rsPost('/fops', $data);
    }

    public function unzipWithFops(string $fops, string $notifyURL = null, int $separate = 0)
    {
        return $this->fops('/fmgr/compress', $fops, $notifyURL, null, $separate);
    }

    /**
     * 查询持久化处理状态
     *
     * @param string $persistentId
     * @return array
     * @link https://wcs.chinanetcenter.com/document/API/ResourceManage/PersistentStatus
     */
    public function persistentStatus(string $persistentId): array
    {
        return Support::get('/status/get/prefop?persistentId=' . $persistentId, []);
    }

    public function persistentStatusWithFops(string $persistentId)
    {
        return Support::get('/fmgr/status?persistentId=' . $persistentId, []);
    }

    private function rsPost($path, $body = null)
    {
        $url = $this->config->getRsfDomain() . $path;
        $headers = $this->auth->authorizationV2($url, $body);
        return Support::post($url, $body, $headers);
    }

    private function rsGet($path)
    {
        $url = $this->config->getRsfDomain() . $path;
        $headers = $this->auth->authorizationV2($url);
        return Support::get($url, $headers);
    }

    protected function fops(string $url, string $fops, string $notifyURL = null, int $force = 1, int $separate = 0)
    {
        $data = ['fops' => $fops];
        if (!empty($notifyURL)) {
            $data['notifyURL'] = Support::base64_urlSafeEncode($notifyURL);
        }
        if (is_numeric($force)) {
            $data['force'] = $force;
        }
        if (is_numeric($separate)) {
            $data['separate'] = $separate;
        }
        return $this->rsPost($url, $data);
    }
}
