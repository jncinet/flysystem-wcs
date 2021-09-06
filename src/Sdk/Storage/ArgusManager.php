<?php

namespace Jncinet\Flysystem\Wcs\Sdk\Storage;

use Jncinet\Flysystem\Wcs\Sdk\Auth;
use Jncinet\Flysystem\Wcs\Sdk\Config;
use Jncinet\Flysystem\Wcs\Sdk\Support;

/**
 * 内容审核接口
 */
final class ArgusManager
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
     * 视频审核
     *
     * @param array $body body信息
     *
     * @return array
     *
     * @link https://wcs.chinanetcenter.com/document/API/Image-op/videoContentAnalysis
     */
    public function censorVideo(array $body)
    {
        $path = '/v1/vca';

        if (array_key_exists('notifyUrl', $body)) {
            $body['notifyUrl'] = Support::base64_urlSafeEncode($body['notifyUrl']);
        }

        return $this->arPost($path, $body);
    }


    /**
     * 图片审核
     *
     * @param array $body
     *
     * @return array
     *
     * @link https://wcs.chinanetcenter.com/document/API/Image-op/imageDetect
     */
    public function censorImage(array $body)
    {
        $path = '/imageDetect';

        $body['image'] = Support::base64_urlSafeEncode($body['image']);

        return $this->arPost($path, $body);
    }

    /**
     * 查询视频审核结果
     *
     * @param string $jobId
     * @return array
     * @throws \GuzzleHttp\Exception\GuzzleException
     * @link  https://developer.qiniu.com/censor/api/5620/video-censor
     */
    public function censorStatus(string $jobId): array
    {
        return $this->get('/v1/vca/search?jobId=' . $jobId);
    }

    private function arPost($path, $body)
    {
        $url = $this->config->getRsfDomain() . $path;
        $headers = $this->auth->authorizationV2($url, $body);
        return Support::post($url, $body, $headers);
    }

    private function get($url)
    {
        $headers = $this->auth->authorizationV2($url);
        return Support::get($url, $headers);
    }
}
