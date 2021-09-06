<?php

namespace Jncinet\Flysystem\Wcs\Sdk;

class Config
{
    const SDK_VER = '2.0.8';

    // 分片大小：4M/片
    const BLOCK_SIZE = 4194304;
    const CHUNK_SIZE = 4194304;
    const COUNT_FOR_RETRY = 3;
    const RECORD_URL = '';

    // 上传域名
    protected $upDomain;
    // 查看域名
    protected $rsDomain;
    // 管理域名
    protected $rsfDomain;

    public function __construct(array $configs = [])
    {
        $this->upDomain = $configs['put_domain'] ?? '';
        $this->rsDomain = $configs['get_domain'] ?? '';
        $this->rsfDomain = $configs['mgr_domain'] ?? '';
    }

    public function getPutDomain()
    {
        return $this->upDomain;
    }

    public function getRsDomain()
    {
        return $this->rsDomain;
    }

    public function getRsfDomain()
    {
        return $this->rsfDomain;
    }
}
