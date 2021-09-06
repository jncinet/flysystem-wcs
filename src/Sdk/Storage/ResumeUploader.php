<?php

namespace Jncinet\Flysystem\Wcs\Sdk\Storage;

use GuzzleHttp\Client;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\RequestException;
use Psr\Http\Message\ResponseInterface;
use GuzzleHttp\Promise;
use Jncinet\Flysystem\Wcs\Sdk\Config;
use Jncinet\Flysystem\Wcs\Sdk\Support;

/**
 * 断点续上传类, 该类主要实现了断点续上传中的分块上传,
 * 以及相应地创建块和创建文件过程.
 *
 * @link https://wcs.chinanetcenter.com/document/API/FileUpload/SliceUpload
 * @link https://wcs.chinanetcenter.com/document/API/FileUpload/AppendUpload
 */
final class ResumeUploader
{
    //需要传入的参数
    private $blockSize;
    private $chunkSize;
    private $countForRetry;

    //用户自定义信息
    private $userParam;
    private $encodedUserVars;
    private $mimeType;

    //断点续传信息
    private $localFile;
    private $chunkNumOfUploaded;
    private $ctxListForMkfile;
    private $sizeOfFile;
    private $sizeOfUploaded;
    private $latestChunkCtx;
    private $time;
    private $hashTable; //上传的全局信息
    private $token;
    private $handle;
    private $rcdFileHandle;
    private $rcdLogHandle;
    private $hash;

    //uuid随机数用 php 的uniqid()
    private $uuid;

    //断点续传记录文件
    private $recordFile;

    //日志文件
    private $recordLog;
    protected $config;

    public function __construct(
        $token,
        Config $config,
        $userParam = null,
        $encodedUserVars = null,
        $mimeType = null
    )
    {
        $this->config = $config;
        $this->blockSize = Config::BLOCK_SIZE;
        $this->chunkSize = Config::CHUNK_SIZE;
        $this->countForRetry = Config::COUNT_FOR_RETRY;
        $this->chunkNumOfUploaded = 0;
        $this->ctxListForMkfile = array();
        $this->ctxList = array();
        $this->sizeOfFile = 0;
        $this->sizeOfUploaded = 0;
        $this->latestChunkCtx = '';
        $this->uuid = '';
        $this->time = 0;
        $this->recordFile = '';
        $this->recordLog = '';
        $this->hash = array();

        //默认用户自定义信息
        $this->token = $token;
        $this->userParam = $userParam;
        $this->encodedUserVars = $encodedUserVars;
        $this->mimeType = $mimeType;
        $this->hashTable = array();
    }


    /**
     * 分片上传
     */
    public function upload($localFile)
    {
        // 默认不覆盖
        return $this->_upload($localFile);
    }

    /**
     * @param $localFile
     * @return mixed
     * @throws \ErrorException
     */
    function _upload($localFile)
    {
        date_default_timezone_set('Asia/Shanghai');
        $this->localFile = $localFile;

        // 记录文件后缀.rcd, WCS_RECORD_URL 为记录文件的路径，默认是上传文件当前路径
        $this->recordFile = Config::RECORD_URL . '.' . basename($localFile) . '.rcd';
        // 日志文件
        $this->recordLog = Config::RECORD_URL . '.' . basename($localFile) . '.log';
        if (!file_exists($this->localFile)) {
            throw new \Exception("ERROR: {$this->localFile}文件不存在！");
        }
        clearstatcache();
        $this->sizeOfFile = Support::getFileSize($localFile);

        //如果有断点续传记录，从记录文件中读取信息
        if (file_exists($this->recordFile) && Support::getFileSize($this->recordFile) != 0) {
            //获取记录文件最后一行的记录
            $result = $this->fileLastLines($this->recordFile);
            if ($result !== false) {
                $result = json_decode($result, true);
                $this->hashTable = $result;
                $info = $result['info'];
                $this->sizeOfUploaded = $info['sizeOfUploaded'];
                $this->ctxListForMkfile = $info['ctxList'];
                $this->uuid = $info['uuid'];
                $this->time = $info['time'];
            } else {
                $this->initResumeInfo();
            }
        } else {
            $this->initResumeInfo();
        }

        return $this->resumeUpload();
    }

    /**
     * 初始化上传信息
     */
    private function initResumeInfo()
    {
        //获取token
        $this->uuid = uniqid();
        $this->time = time();

        //初始化基本信息
        $info = array(
            'sizeOfUploaded' => 0,
            'sizeOfFile' => $this->sizeOfFile,
            'progress' => 0,
            'uuid' => $this->uuid,
            'token' => $this->token,
            'time' => $this->time,
            'ctxList' => array()
        );
        $this->hashTable['info'] = $info;
    }

    // 返回值需处理
    private function resumeUpload()
    {
        $recordLog = fopen($this->recordLog, "a");
        if ($recordLog === false) {
            throw new \Exception("ERROR: {$this->recordLog}文件打开失败！");
        }
        $this->rcdLogHandle = $recordLog;

        // 打开文件，指定从断点地方开始读取
        $handle = fopen($this->localFile, "r");
        if ($handle === false) {
            fwrite($recordLog, date('Y-m-d H:i:s') . " " . "ERROR:{$this->localFile}文件打开失败\n");
            throw new \Exception("ERROR: {$this->localFile}文件打开失败！");
        }
        $this->handle = $handle;

        $recordFile = fopen($this->recordFile, "a");
        if ($recordFile === false) {
            fwrite($recordLog, date('Y-m-d H:i:s') . " " . "ERROR:{$this->recordFile}文件打开失败！\n");
            throw new \Exception("ERROR:{$this->recordFile}文件打开失败！");
        }
        $this->rcdFileHandle = $recordFile;

        // 片大小必须是块大小的整数倍
        $blockNum = ceil($this->sizeOfFile / ($this->blockSize));

        $client = new Client(['User-Agent' => Support::userAgent()]);

        $requests = function ($blockNum) {
            // hash 块上传的顺序
            $count = 0;
            for ($i = 0; $i < $blockNum; $i++) {
                if (isset($this->hashTable[$i]['chunk']) && $this->hashTable[$i]['chunk'] > 0) {
                    continue;
                }
                $this->hash[$count++] = $i;
            }

            for ($curBlockNum = 0; $curBlockNum < $blockNum; $curBlockNum++) {
                if (isset($this->hashTable[$curBlockNum]['success'])) {
                    $uploadStatus = $this->hashTable[$curBlockNum]['success'];
                    if ($uploadStatus == true) {
                        continue;
                    }
                }

                if (isset($this->hashTable[$curBlockNum]['chunk'])) {
                    if ($this->hashTable[$curBlockNum]['chunk'] > 0) {
                        $this->uploadBlockResume($curBlockNum);
                        continue;
                    }
                }

                // 判断是否是最后一块，如果是最后一块，按照块的实际大小
                if ($curBlockNum == $blockNum - 1) {
                    $curBlockSize = ($this->sizeOfFile - $curBlockNum * $this->blockSize);
                } else {
                    $curBlockSize = $this->blockSize;
                }

                $chunkNum = ceil(($curBlockSize) / ($this->chunkSize));

                $url = $this->config->getPutDomain() . '/mkblk/' . $curBlockSize . '/' . $curBlockNum;
                // 如果当前文件剩余内容小于chunkSize,只会读取到EOF
                // 定位到文件上次中断的位置
                $offset = round($curBlockNum * $this->blockSize, 0);

                if (fseek($this->handle, $offset, SEEK_SET) == -1) {
                    fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "ERROR:读取文件出错！\n");
                    throw new \Exception("ERROR:读取文件出错！");
                }
                $curChunk = fread($this->handle, $this->chunkSize);
                $curChunkSize = strlen($curChunk);
                $chunk = 0;
                $uploaded = 0;

                $this->hashTable[$curBlockNum] = array(
                    'success' => false,
                    'blockSize' => $curBlockSize,
                    'curChunkSize' => $curChunkSize,
                    'chunkNum' => $chunkNum,
                    'chunk' => $chunk,
                    'uploaded' => $uploaded,
                    'latestCtx' => '',
                    'retry' => 3
                );

                if (!isset($this->hashTable[$i]) || $this->hashTable[$i]['success'] == false) {
                    yield new Request('POST', $url, [
                        'Authorization' => $this->token,
                        'User-Agent' => Support::userAgent(),
                        'Content-Type' => 'application/octet-stream',
                        'uploadBatch' => $this->uuid
                    ], $curChunk);
                }
            }
        };

        $pool = new Pool($client, $requests($blockNum), [
            'concurrency' => 5,
            'fulfilled' => function (ResponseInterface $response, $index) {
                $index = $this->hash[$index];
                if ($response->getStatusCode() == 200) {
                    $this->makeBlockResume($response->getBody(), $index);
                }
            },
            'rejected' => function (RequestException $e, $index) {
                if ($e->hasResponse() == false) {
                    fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "请求超时！" . "\n");

                    $index = $this->hash[$index];
                    $offset = $index * $this->blockSize;
                    if (fseek($this->handle, $offset, SEEK_SET) == -1) {
                        fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "ERROR:读取文件出错！\n");
                        throw new \Exception("ERROR:读取文件出错！");
                    }
                    $curChunk = fread($this->handle, $this->chunkSize);

                    //判断是否是最后一块，如果是最后一块，按照块的实际大小
                    $curBlockSize = ($this->sizeOfFile - ($index * $this->blockSize)) > $this->blockSize
                        ? $this->blockSize
                        : $this->sizeOfFile - ($index * $this->blockSize);

                    $resp = $this->mackBlockTimeout($curBlockSize, $index, $this->token, $curChunk, $this->rcdLogHandle);

                    if ($resp['code'] == 200) {
                        $this->makeBlockResume($resp->respBody, $index);
                    } else {
                        fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "超时重试失败！" . "\n");
                    }

                } else {
                    fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . json_encode($e->getMessage()) . "\n");
                    throw new \Exception($e->getMessage(), $e->getCode(), $e->getPrevious());
                }
            },
        ]);

        //等待所有并发请求返回
        $promise = $pool->promise();
        $promise->wait();

        if (Support::getFileSize($this->recordFile) == 0) {
            $this->deleteRecord($this->recordFile);
        }
        // 检验上传文件完整性
        if ($this->hashTable['info']['sizeOfUploaded'] == $this->sizeOfFile) {
            $resp = $this->makeFile($this->token);

            //超时重试
            if ($resp['code'] == 28) {
                $resp = $this->makeFileTimeout($this->token, $recordLog);
            }

            $this->makeFileHandler($resp, $recordLog);
        } else {
            fwrite($recordLog, date('Y-m-d H:i:s') . " " . "ERROR:上传过程中出现异常，请重新续传！\n");
            throw new \Exception("ERROR:上传过程中出现异常，请重新续传！");
        }

        fclose($recordFile);
        fclose($handle);
        return $resp;
    }

    /**
     * @param $body
     * @param $index
     * @return void
     * @throws \Exception
     */
    private function makeBlockResume($body, $index)
    {
        $result = json_decode($body, true);
        $latestCtx = $result['ctx'];
        $this->hashTable[$index]['latestCtx'] = $latestCtx;
        $this->hashTable[$index]['chunk'] += 1;
        $this->hashTable[$index]['uploaded'] += $this->hashTable[$index]['curChunkSize'];
        $chunk = $this->hashTable[$index]['chunk'];
        $chunkNum = $this->hashTable[$index]['chunkNum'];

        $this->hashTable['info']['sizeOfUploaded'] +=
            $this->hashTable[$index]['curChunkSize'];

        //记录上传进度
        $this->hashTable['info']['progress'] =
            $this->hashTable['info']['sizeOfUploaded'] / $this->hashTable['info']['sizeOfFile'] * 100;

        // 给文件加排它锁
        if (flock($this->rcdFileHandle, LOCK_EX)) {
            fwrite($this->rcdFileHandle, json_encode($this->hashTable) . "\n");
            flock($this->rcdFileHandle, LOCK_UN);
        }

        for ($i = $chunk; $i < $chunkNum; $i++) {
            //bput
            $offset = $i * $this->chunkSize + $index * $this->blockSize;
            if (fseek($this->handle, $offset, SEEK_SET) == -1) {
                fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "ERROR:读取文件出错！\n");
                throw new \Exception("ERROR:读取文件出错！");
            }
            //如果当前文件剩余内容小于chunkSize,只会读取到EOF
            $curChunk = fread($this->handle, $this->chunkSize);
            $curChunkSize = strlen($curChunk);
            $latestCtx = $this->hashTable[$index]['latestCtx'];
            $chunkOffset = $i * $this->chunkSize;
            $resp = $this->uploadBlock($latestCtx, $chunkOffset, $this->token, $curChunk);

            //超时重试
            if ($resp['code'] == 28) {
                $resp = $this->blockUploadTimeout($latestCtx, $chunkOffset, $this->token, $curChunk, $index, $this->rcdLogHandle);
            }
            $this->uploadBlockHandler($resp, $index, $curChunkSize);
        }
    }

    /**
     * @param $index
     * @return void
     * @throws \ErrorException
     */
    function uploadBlockResume($index)
    {
        $chunk = $this->hashTable[$index]['chunk'];
        $chunkNum = $this->hashTable[$index]['chunkNum'];

        for ($i = $chunk; $i < $chunkNum; $i++) {
            $offset = $index * $this->blockSize + $i * $this->chunkSize;
            $latestCtx = $this->hashTable[$index]['latestCtx'];

            if (fseek($this->handle, $offset, SEEK_SET) == -1) {
                fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "ERROR:读取文件出错！\n");
                throw new \Exception("ERROR:读取文件出错！");
            }
            $curChunk = fread($this->handle, $this->chunkSize);
            $curChunkSize = strlen($curChunk);
            $chunkOffset = $i * $this->chunkSize;
            $resp = $this->uploadBlock($latestCtx, $chunkOffset, $this->token, $curChunk);
            //超时重试
            if ($resp['code'] == 28) {
                $resp = $this->blockUploadTimeout($latestCtx, $chunkOffset, $this->token, $curChunk, $index, $this->rcdLogHandle);
            }
            $this->uploadBlockHandler($resp, $index, $curChunkSize);
        }

        if ($this->hashTable[$index]['uploaded'] !== $this->hashTable[$index]['blockSize']) {
            throw new \ErrorException('块上传校验失败！');
        }
    }

    /**
     * 块上传
     */
    function makeBlock($curBlockSize, $curBlockNum, $token, $nextChunk)
    {
        $url = $this->config->getPutDomain() . '/mkblk/' . $curBlockSize . '/' . $curBlockNum;

        $headers = [
            'Authorization' => $token,
            'Content-Type' => 'application/octet-stream',
            'uploadBatch' => $this->uuid,
        ];

        return Support::multipartPost($url, $nextChunk, '', null, $headers);
    }

    /**
     * bput操作，片上传
     */
    function uploadBlock($ctx, $nextChunkOffset, $token, $nextChunk)
    {
        $url = $this->config->getPutDomain() . '/bput/' . $ctx . '/' . $nextChunkOffset;
        $mimeType = null;

        $headers = [
            'Authorization' => $token,
            'Content-Type' => 'application/octet-stream',
            'uploadBatch' => $this->uuid,
        ];

        return Support::multipartPost($url, $nextChunk, '', $mimeType, $headers);
    }

    /**
     * mkfile操作，进行块重组成文件，
     */
    function makeFile($token)
    {
        $url = $this->config->getPutDomain() . '/mkfile/' . $this->sizeOfFile . '/';

        if ($this->userParam !== null) {
            $url .= $this->userParam . '/';
        }

        if ($this->userParam !== null && $this->encodedUserVars != null) {
            $url .= $this->encodedUserVars;
        }

        $headers = [
            'Authorization' => $token,
            'Content-Type' => 'text/plain;charset=UTF-8',
            'uploadBatch' => $this->uuid,
            'key' => Support::base64_urlSafeEncode(basename($this->localFile)),
            'mimeType' => $this->mimeType,
        ];

        $blockNum = ceil($this->sizeOfFile / ($this->blockSize));
        for ($i = 0; $i < $blockNum; $i++) {
            array_push($this->hashTable['info']['ctxList'], $this->hashTable[$i]['latestCtx']);
        }
        $fields = implode(',', $this->hashTable['info']['ctxList']);
        return Support::post($url, $fields, $headers);
    }

    /**
     * mkblk操作后的处理函数
     */
    function makeBlockHandler($resp, $curChunkSize, $recordLog)
    {
        if ($resp->code == 200) {
            $this->chunkNumOfUploaded++;
            $result = json_decode($resp->respBody, true);
            $this->latestChunkCtx = $result['ctx'];
            $this->sizeOfUploaded += $curChunkSize;

            return true;
        } else {
            if (Support::getFileSize($this->recordFile) == 0) {
                $this->deleteRecord($this->recordFile);
            }
            if ($resp->code == 28) {
                fwrite($recordLog, date('Y-m-d H:i:s') . " " . "请求超时！" . "\n");
                die('请求超时！');
            } else {
                fwrite($recordLog, date('Y-m-d H:i:s') . " " . json_encode($resp) . "\n");
                die($resp->respBody);
            }
        }
    }

    /**
     * bput操作后的处理函数
     */
    function uploadBlockHandler($resp, $index, $curChunkSize)
    {
        if ($resp->code == 200) {
            $this->hashTable[$index]['uploaded'] += $curChunkSize;
            $this->hashTable[$index]['curChunkSize'] = $curChunkSize;
            $this->hashTable[$index]['chunk'] += 1;

            $result = json_decode($resp->respBody, true);
            $this->hashTable[$index]['latestCtx'] = $result['ctx'];

            $this->hashTable['info']['sizeOfUploaded'] +=
                $this->hashTable[$index]['curChunkSize'];

            if ($this->hashTable[$index]['uploaded'] == $this->hashTable[$index]['blockSize']) {
                $this->hashTable[$index]['success'] = true;
            }

            //记录上传进度
            $this->hashTable['info']['progress'] =
                $this->hashTable['info']['sizeOfUploaded'] / $this->hashTable['info']['sizeOfFile'] * 100;

            //是否输出上传进度
            print_r("progress: " . $this->hashTable['info']['progress'] . "%\n");

            fwrite($this->rcdFileHandle, json_encode($this->hashTable) . "\n");

            return true;
        } else {
            if ($resp->code == 28) {
                fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . "请求超时！" . "\n");
                throw new \Exception('请求超时！');
            } else {
                fwrite($this->rcdLogHandle, date('Y-m-d H:i:s') . " " . json_encode($resp) . "\n");
                throw new \Exception($resp->respBody);
            }
        }
    }

    /**
     * mkfile操作后的处理函数
     */
    function makeFileHandler($resp, $recordLog)
    {
        if ($resp->code == 200) {
            $this->deleteRecord($this->recordFile);
            $this->deleteRecord($this->recordLog);

            return $resp;
        } else {
            if ($resp->code == 28) {
                fwrite($recordLog, date('Y-m-d H:i:s') . " " . "请求超时！" . "\n");
                die('请求超时！');
            } else {
                fwrite($recordLog, date('Y-m-d H:i:s') . " " . json_encode($resp) . "\n");
                die($resp->respBody);
            }

        }
    }

    /**
     * 块超时重试处理
     */
    function mackBlockTimeout($curBlockSize, $curBlockNum, $token, $curChunk, $recordLog)
    {
        while ($this->hashTable[$curBlockNum]['retry']) {

            //echo "mkblk{$curBlockNum}超时重试{$this->hashTable[$curBlockNum]['retry']}\n";
            $resp = $this->makeBlock($curBlockSize, $curBlockNum, $token, $curChunk);

            if ($resp['code'] !== 28) {
                //重置countForRetry
                $this->hashTable[$curBlockNum]['retry'] = 3;
                return $resp;
            }

            $this->hashTable[$curBlockNum]['retry']--;
        }
        fwrite($recordLog, date('Y-m-d H:i:s') . " " . "ERROR:上传超时，重试失败！\n");
        return $resp;
    }

    /**
     *分片上传超时重试
     */
    function blockUploadTimeout($latestChunkCtx, $offsetOfChunk, $token, $curChunk, $curBlockNum, $recordLog)
    {
        while ($this->hashTable[$curBlockNum]['retry']) {

            echo "mkblk{$curBlockNum}-bput超时重试{$this->hashTable[$curBlockNum]['retry']}\n";
            $resp = $this->uploadBlock($latestChunkCtx, $offsetOfChunk, $token, $curChunk);

            if ($resp['code'] !== 28) {
                //重置countForRetry
                $this->hashTable[$curBlockNum]['retry'] = 3;
                return $resp;
            }

            $this->hashTable[$curBlockNum]['retry']--;
        }
        fwrite($recordLog, date('Y-m-d H:i:s') . " " . "ERROR:上传超时，重试失败！\n");
        return $resp;
    }


    /**
     * mkfile操作超时重试
     */
    function makeFileTimeout($token, $recordLog)
    {
        while ($this->countForRetry) {
            echo "超时重试{$this->countForRetry}\n";
            $resp = $this->makeFile($token);

            if ($resp['code'] !== 28) {
                //重置countForRetry
                $this->countForRetry = 3;
                return $resp;
            }

            $this->countForRetry--;
        }

        fwrite($recordLog, date('Y-m-d H:i:s') . " " . "ERROR:上传超时，重试失败！\n");
        return $resp;
    }

    /**
     * 记录文件删除
     *
     * @param $recordFile
     * @throws \Exception
     */
    function deleteRecord($recordFile)
    {
        if (is_file($recordFile)) {
            if (!unlink($recordFile)) {
                throw new \Exception("文件{$recordFile}删除失败！");
            }
        }
    }

    /**
     * 读取文件最后一行
     *
     * @param $fineName
     * @return false|string
     */
    private function fileLastLines($fineName)
    {
        $fp = fopen($fineName, 'r');
        if (!$fp) {
            return false;
        }

        $offset = -1;
        $endChar = '';
        $string = '';
        while (!$string) {
            while ($endChar != "\n") {
                // 0-成功  1-失败
                if (fseek($fp, $offset, SEEK_END) == 0) {
                    $endChar = fgetc($fp);
                    $offset--;
                } else {
                    break;
                }
            }
            $string = fgets($fp);
            if ($string == '' || $string == "\n") {
                $string = '';
            }
            $endChar = '';
        }
        fclose($fp);
        return $string;
    }
}
