<?php

namespace Jncinet\Flysystem\Wcs\Sdk\Storage;

use Jncinet\Flysystem\Wcs\Sdk\Config;
use Jncinet\Flysystem\Wcs\Sdk\Support;

class ResumeUpload
{
    private $upToken; // 上传凭证
    private $key; // 上传文件名
    private $inputStream; // 上传二进制流
    private $size; // 上传流的大小
    private $params; // 自定义变量
    private $mime; // 上传数据的mimeType
    private $contexts;
    private $finishedEtags;
    private $host; // 上传文件域名
    private $currentUrl;
    private $config;
    private $resumeRecordFile; // 断点续传的已上传的部分信息记录文件
    private $partSize; // 默认大小为4MB

    public function __construct($upToken,
        $key,
        $inputStream,
        $size,
        $params,
        $mime,
                                Config $config,
        $resumeRecordFile = null,
        $partSize = Config::BLOCK_SIZE)
    {
        $this->upToken = $upToken;
        $this->key = $key;
        $this->inputStream = $inputStream;
        $this->size = $size;
        $this->params = $params;
        $this->mime = $mime;
        $this->contexts = array();
        $this->finishedEtags = ['etags' => [], 'uploadId' => '', 'expiredAt' => 0, 'uploaded' => 0];
        $this->config = $config;
        $this->resumeRecordFile = $resumeRecordFile ?: null;
        $this->partSize = $partSize ?: config::BLOCK_SIZE;
        $this->host = $config->getPutDomain();
    }

    /**
     * 创建块
     */
    public function makeBlock($block, $blockSize)
    {
        $url = $this->host . '/mkblk/' . $blockSize;
        return Support::post($url, $block);
    }
}
