<?php

namespace Jncinet\Flysystem\Wcs\Sdk;

class Auth
{
    private $accessKey;
    private $secretKey;

    public function __construct(string $accessKey, string $secretKey)
    {
        $this->accessKey = $accessKey;
        $this->secretKey = $secretKey;
    }

    /**
     * @return string
     */
    public function getAccessKey(): string
    {
        return $this->accessKey;
    }

    /**
     * @param string $data
     * @return string
     */
    public function sign(string $data): string
    {
        $hmac = hash_hmac('sha1', $data, $this->secretKey, false);
        return $this->accessKey . ':' . Support::base64_urlSafeEncode($hmac);
    }

    /**
     * @param string $data
     * @return string
     */
    public function signWithData(string $data): string
    {
        $encodedData = Support::base64_urlSafeEncode($data);
        return $this->sign($encodedData) . ':' . $encodedData;
    }

    /**
     * @param string $urlString
     * @param $body
     * @return string
     */
    public function signRequest(string $urlString, $body): string
    {
        $url = parse_url($urlString);
        $data = '';
        if (array_key_exists('path', $url)) {
            $data = $url['path'];
        }
        if (array_key_exists('query', $url)) {
            $data .= '?' . $url['query'];
        }
        $data .= "\n";

        if ($body !== null) {
            $data .= $body;
        }
        return $this->sign($data);
    }

    public function verifyCallback($contentType, $originAuthorization, $url, $body)
    {
        $authorization = 'QBox ' . $this->signRequest($url, $body, $contentType);
        return $originAuthorization === $authorization;
    }

    public function privateDownloadUrl($baseUrl, $expires = 3600)
    {
        $deadline = time() + $expires;

        $pos = strpos($baseUrl, '?');
        if ($pos !== false) {
            $baseUrl .= '&e=';
        } else {
            $baseUrl .= '?e=';
        }
        $baseUrl .= $deadline;

        $token = $this->sign($baseUrl);
        return "$baseUrl&token=$token";
    }

    /**
     * 上传凭证
     *
     * @param $bucket
     * @param null $key
     * @param int $expires
     * @param array|null $policy
     * @param bool $strictPolicy
     * @return string
     *
     * @link https://wcs.chinanetcenter.com/document/API/Token/UploadToken
     */
    public function uploadToken($bucket, $key = null, int $expires = 3600, array $policy = null, bool $strictPolicy = true): string
    {
        $deadline = round(1000 * (microtime(true) + $expires), 0);
        $scope = $bucket;
        if ($key !== null) {
            $scope .= ':' . $key;
        }

        $args = self::copyPolicy($args, $policy, $strictPolicy);
        $args['scope'] = $scope;
        $args['deadline'] = $deadline;

        $b = json_encode($args);
        return $this->signWithData($b);
    }

    /**
     * 上传策略，参数规格详见
     *
     * @link https://wcs.chinanetcenter.com/document/API/Token/UploadToken
     */
    private static $policyFields = array(
        /* @link https://wcs.chinanetcenter.com/document/API/Token/PutPolicy/savekey */
        'saveKey',
        'returnUrl',
        'returnBody',
        'overwrite',
        'fsizeLimit',

        'callbackUrl',
        'callbackBody',

        'persistentOps',
        'persistentNotifyUrl',

        'contentDetect',
        'detectNotifyURL',
        'detectNotifyRule',
        'separate',
    );

    private static function copyPolicy(&$policy, $originPolicy, $strictPolicy)
    {
        if ($originPolicy === null) {
            return array();
        }
        foreach ($originPolicy as $key => $value) {
            if (!$strictPolicy || in_array((string)$key, self::$policyFields, true)) {
                $policy[$key] = $value;
            }
        }
        return $policy;
    }

    public function authorization($url, $body = null, $contentType = null)
    {
        $authorization = 'QBox ' . $this->signRequest($url, $body, $contentType);
        return array('Authorization' => $authorization);
    }

    public function authorizationV2($url, $body = null): array
    {
        $path = parse_url($url, PHP_URL_PATH);
        $query = parse_url($url, PHP_URL_QUERY);
        if ($query) {
            if ($body) {
                $arr = array($path, '?', $query, "\n", $body);
            } else {
                $arr = array($path, '?', $query, "\n");
            }
        } else {
            if ($body) {
                $arr = array($path, "\n", $body);
            } else {
                $arr = array($path, "\n");
            }
        }
        $toSignStr = implode('', $arr);
        return array('Authorization' => $this->sign($toSignStr));
    }
}
