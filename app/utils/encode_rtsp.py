import urllib.parse

def encode_rtsp_url(url: str) -> str:
    """
    URL-encode RTSP credentials to handle special characters.
    Prevents double-encoding by checking if URL is already encoded.
    
    Example:
        Input:  rtsp://admin:G@tew0rx@192.168.1.1/stream
        Output: rtsp://admin:G%40tew0rx@192.168.1.1/stream
    """
    try:
        # ✅ CRITICAL: Check if already encoded
        # If URL contains %XX pattern, it's likely already encoded
        if '%' in url:
            # Try to decode first to see if it's valid encoding
            try:
                decoded = urllib.parse.unquote(url)
                # If decode changed something, it was encoded
                if decoded != url:
                    logger.debug(f"URL already encoded, using as-is: {url}")
                    return url
            except Exception:
                pass
        
        parsed = urllib.parse.urlparse(url)
        
        # Only encode if there's authentication
        if '@' in parsed.netloc and ':' in parsed.netloc.split('@')[0]:
            auth, host = parsed.netloc.rsplit('@', 1)
            username, password = auth.split(':', 1)
            
            # ✅ Only encode if password contains special characters
            if any(c in password for c in ['@', ':', '/', '?', '#', '[', ']']):
                # Encode password (safe characters for URL schemes)
                encoded_password = urllib.parse.quote(password, safe='')
                encoded_auth = f"{username}:{encoded_password}"
                encoded_netloc = f"{encoded_auth}@{host}"
                
                encoded_url = urllib.parse.urlunparse((
                    parsed.scheme,
                    encoded_netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment
                ))
                
                logger.info(f"🔒 Encoded RTSP URL: {encoded_url}")
                return encoded_url
        
        return url
    except Exception as e:
        logger.error(f"Error encoding RTSP URL: {e}")
        return url