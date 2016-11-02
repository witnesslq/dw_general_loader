package com.angejia.dw.biz;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

public class UrlParser
{
    
    public static String getUrlFromJson(JSONObject jsonObj, String field)
    {
        String url = jsonObj.getString(field);
        if (url.lastIndexOf("`") >= 0)
            url = url.replaceAll("`", "");
        return url.length() > 255 ? url.substring(0, 255) : url;
    }

    public static String getKeywordFromUrl(String url, String site) throws UnsupportedEncodingException
    {
        try
        {
            // 安居客问答搞特殊化
            String ajkQAURLPath = "anjuke.com/ask/Q";
            String ajkQAURLPath1 = "anjuke.com/ask/S";
            if (url.indexOf(ajkQAURLPath) > 0 || url.indexOf(ajkQAURLPath1) > 0)
            {	
            	String kwTMP ="";
            	if(url.indexOf(ajkQAURLPath) > 0){
            		kwTMP = url.substring(url.indexOf(ajkQAURLPath) + ajkQAURLPath.length());
            	}else{
            		kwTMP = url.substring(url.indexOf(ajkQAURLPath1) + ajkQAURLPath1.length());
            	}
                
                if (kwTMP.indexOf("-") > 0)
                    kwTMP = kwTMP.substring(0, kwTMP.indexOf("-"));
                
                String kwFinal = decodeWendaKeyword(kwTMP);
                /*
                 * 这段用新逻辑替代-2013-08-16
                // +号代表空格
                String[] kwTMPs = kwTMP.split("\\+");

                String kwFinal = "";
                for (int s = 0; s < kwTMPs.length; s++)
                {
                    String tmp = kwTMPs[s];
                    char[] chs = tmp.toCharArray();
                    if (chs.length % 2 != 0)// 不正常
                        return "";

                    // 添加%
                    StringBuffer keywordSB = new StringBuffer();
                    for (int i = 0; i < chs.length; i += 2)
                    {
                        keywordSB.append("%");
                        keywordSB.append(chs[i]);
                        keywordSB.append(chs[i + 1]);
                    }

                    String keyword = URLDecoder.decode(keywordSB.toString(), "utf8");
                    kwFinal += " ";
                    kwFinal += (keyword.length() > 100 ? keyword.substring(0, 90) : keyword);
                }*/
                kwFinal = (kwFinal.length() > 100 ? kwFinal.substring(0, 90) : kwFinal);
                return kwFinal;
            }
        }
        catch (Exception e)
        {
            return "";
        }

        String keyword = findKeywordFromURL(url);
        if(keyword == null) return "";
        return keyword.length() > 100 ? keyword.substring(0, 90) : keyword;
    }

    public static String decodeWendaKeyword(String keyword) {
    	//为了问答做的解析特殊处理,按照线上解析逻辑打造的
        if (keyword == null || keyword.length() == 0) {
            return "";
        }

        keyword = keyword.replace("+", "20");

        StringBuilder sb = new StringBuilder();
        Pattern ptrnAlphabet = Pattern.compile("z[a-zA-Z]");
        Pattern ptrnUtf8 = Pattern.compile("[8-9A-F][0-9A-F]");

        try {

            for (int i = 0; i < keyword.length(); i += 2) {

                String two = keyword.substring(i, i + 2);
                if (ptrnAlphabet.matcher(two).matches()) {
                    sb.append(keyword.charAt(i + 1));
                } else if (ptrnUtf8.matcher(two).matches()) {

                    if (ptrnUtf8.matcher(keyword.substring(i + 2, i + 4)).matches()
                            && ptrnUtf8.matcher(keyword.substring(i + 4, i + 6)).matches()) {

                        byte[] bytes = new byte[3];
                        for (int j = 0; j < 3; ++j) {
                            bytes[j] = (byte) Integer.parseInt(keyword.substring(i + j * 2, i + j * 2 + 2), 16);
                        }
                        sb.append(new String(bytes, "UTF-8"));

                    } else {
                        throw new IllegalArgumentException();
                    }
                    i += 4;

                } else {
                    byte[] bytes = new byte[1];
                    bytes[0] = (byte) Integer.parseInt(keyword.substring(i, i + 2), 16);
                    sb.append(new String(bytes, "UTF-8"));
                }

            }

        } catch (Exception e) {
            return "";
        }

        return sb.toString();

    }
    
    private static String findKeywordFromURL(String url) throws UnsupportedEncodingException
    {
        url = url.toLowerCase();

        String keyword = null;
        
        String encoding = getParamValueByName(url, "ie");
        
        if(encoding != null)
        {
            if (encoding.equals(""))
                encoding = null;
            else if (encoding.startsWith("gb"))
                encoding = "gbk";
        }
      

        if (url.indexOf("baidu.com") != -1 || url.startsWith("http://119.75.213.61/"))
        {
            // baidu的当前搜索关键字的参数名，普通为wd，
            //当通过第三方站点（如hao123的百度搜索栏）时、或通过百度知道的广告时可能为word。
            keyword = getSpecifiedKeyword(url, "wd", encoding, "gbk");
            if (keyword == null)
                keyword = getSpecifiedKeyword(url, "word", encoding, "gbk");
        }
        
        else if (url.indexOf("google.") != -1)
            keyword = getSpecifiedKeyword(url, "q", encoding, "utf8");
        
        else if (url.indexOf("sogou.") != -1)
            keyword = getSpecifiedKeyword(url, "query", encoding, "gbk");
        
        else if (url.indexOf("soso.com") != -1 || url.indexOf("qq.com") != -1)
        {
            boolean isWenWen = url.indexOf("wenwen.soso") >= 0;
            keyword = getSpecifiedKeyword(url, isWenWen ? "sp" : "w", encoding, isWenWen ? "utf8" : "gbk");
        }
        
        else if (url.indexOf(".bing.") != -1)
            keyword = getSpecifiedKeyword(url, "q", encoding, "utf8");
        
        else if (url.indexOf("iask.sina") != -1)
            keyword = getSpecifiedKeyword(url, "key", encoding, "gbk");
        
        else if (url.indexOf("baixing.com") != -1)
            keyword = getSpecifiedKeyword(url, "query", encoding, "utf8");
        
        else if (url.indexOf("114search.118114.cn") != -1)
            keyword = getSpecifiedKeyword(url, "kw", encoding, "gbk");
        
        else if (url.indexOf("www.95i.cn") != -1)
            keyword = getSpecifiedKeyword(url, "keyword", encoding, "gbk");

        else if (url.indexOf("www.ivc.cn") != -1)
            keyword = getSpecifiedKeyword(url, "wd", encoding, "gbk");

        else if (url.indexOf(".smarter.com.cn") != -1)
            keyword = getSpecifiedKeyword(url, "q", encoding, "gbk");

        else if (url.indexOf(".wnpso.com") != -1)
            keyword = getSpecifiedKeyword(url, "bs", encoding, "gbk");

        else if (url.indexOf(".99fang.") != -1)
            keyword = getSpecifiedKeyword(url, "q", encoding, "utf8");

        else if (url.indexOf(".youdao.") != -1)
            keyword = getSpecifiedKeyword(url, "q", encoding, "utf8");
        
        else if (url.indexOf(".58.com") != -1)
        {
            keyword = getSpecifiedKeyword(url, "key", encoding, "utf8");
            
            if (keyword == null)
                keyword = getSpecifiedKeyword(url, "key2", encoding, "utf8");

            if (keyword == null)
                keyword = getSpecifiedKeyword(url, "key5", encoding, "utf8");

            if (keyword == null)
                keyword = getSpecifiedKeyword(url, "key6", encoding, "utf8");

            if (keyword == null)
                keyword = getSpecifiedKeyword(url, "xiaoqu", encoding, "utf8");
        }
        
        // 解析站内搜索关键字
        if (keyword == null && url.contains("anjuke.com")) {
            keyword = findAnjukeKeyword(url, encoding, "utf-8");
        }

        //常规解析策略，不根据参数寻找
        if (keyword == null)
            keyword = findKeywordWithoutParamName(url, encoding);

        // 如果没抓到文字，只是抓到一些其他字符
        if (keyword != null)
        {
            if(keyword.length() == 0)
            {
                keyword = null;
            }    
            else
            {
                char c = keyword.charAt(0);
                if (!Character.isLetter(c) && (c < '0' || c > '9'))
                    keyword = null;
            }
        }

        return keyword;
    }

    private static String getSpecifiedKeyword(String src, String paramName, String encoding, String defaultEncoding)
            throws UnsupportedEncodingException
    {
        String keyword = getParamValueByName(src, paramName);

        if (keyword == null || "".equals(keyword))
            return null;

        // 形如u5047%u65E5%u5609%u56ED的状况(unicode)
        if (keyword.startsWith("%u"))
            return parseUniCode(keyword);

        if (encoding == null)
            encoding = defaultEncoding;

        if (!checkStrEncoding(keyword, encoding))
            keyword = tryEncoding(keyword);
        else
            keyword = URLDecoder.decode(keyword, encoding);

        return keyword;
    }
    
    private static String parseUniCode(String keyword) throws UnsupportedEncodingException
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyword.length(); i += 6)
        {
            try
            {
                // 如果中间还夹杂未转换的字符（如数字）
                if (keyword.charAt(i) != '%')
                {
                    int idx = keyword.indexOf('%', i);
                    if (idx < 0)
                    {
                        sb.append(keyword.substring(i, keyword.length()));
                        break;
                    }
                    else
                    {
                        sb.append(keyword.substring(i, idx));
                        i = idx;
                    }
                }
                
                // 如果中间夹杂符号（如%20）
                while (keyword.charAt(i + 1) != 'u')
                {
                    sb.append(URLDecoder.decode(keyword.substring(i, i + 3), "utf8"));
                    i += 3;
                }
                
                int character = Integer.parseInt(keyword.substring(i + 2, i + 6), 16);
                sb.append((char) character);
            }
            catch (StringIndexOutOfBoundsException e)
            {
                break;
            }
        }
        return  sb.toString();
    }

    //解析url中参数的值
    private static String getParamValueByName(String url, String paramName)
    {
        if (url == null)
            return null;

        // 找到参数位置
        String findString1 = "?" + paramName + "=";
        String findString2 = "&" + paramName + "=";

        String findString = url.indexOf(findString1) >= 0 ? findString1 : findString2;
        int findStringIndex = url.indexOf(findString1) >= 0 ? url.indexOf(findString1) : url.indexOf(findString2);
        if (findStringIndex < 0)
            return null;

        // 得到值的开始位置
        int startPos = findStringIndex + findString.length();
        // 得到值的結束位置，也就是第2个&号的位置
        int endPos = url.indexOf('&', startPos);
        
        // 返回的内容为&utm_term 或者 &kw 或者&tip的内容
        return url.substring(startPos, endPos >= 0 ? endPos : url.length());
    }

    //常规解析，不会识别非中文关键字
    private static String findKeywordWithoutParamName(String src, String encoding) throws UnsupportedEncodingException
    {
        Pattern p = Pattern.compile("=%[a-f||[A-F]||[0-9]]{2}");
        Matcher m = p.matcher(src);
        int parm = 1;
        if (!m.find()){
        	p = Pattern.compile("w0qqkwz%[a-f||[A-F]||[0-9]]{2}");
        	m = p.matcher(src);
            if (!m.find())	
            	return null;
            parm = 7;
        }
        

        // 开始位置
        int startPos = m.start() + parm;

        // 寻找结束位置
        int endPos = startPos;
        while (endPos < src.length() && src.charAt(endPos) == '%'){
        	if((endPos+3)< src.length()&& src.charAt(endPos+3) == '+'){
        		endPos += 4;
        	}else{
        		endPos += 3;
        	}
        }
        String keyword = src.substring(startPos, endPos);

        if (encoding == null)
            keyword = tryEncoding(keyword);
        else
            keyword = URLDecoder.decode(keyword, encoding);

        return keyword;
    }
    
    // 解析站内搜索关键字
    private static String findAnjukeKeyword(String src, String encoding, String defaultEncoding)
            throws UnsupportedEncodingException {
        
        String keyword = null;
        
        // remove url hash
        int sharpIndex = src.indexOf("#");
        if (sharpIndex != -1) {
            src = src.substring(0, sharpIndex);
        }
        
        // kw=123
        keyword = getSpecifiedKeyword(src, "kw", encoding, "utf-8");
        if (keyword != null) {
            return keyword;
        }
        
        // kw_search_input=123
        keyword = getSpecifiedKeyword(src, "kw_search_input", encoding, "utf-8");
        if (keyword != null) {
            return keyword;
        }
        
        do {
            
            Matcher matcher;
            
            // kw123-
            matcher = Pattern.compile("kw([^/\\?]+?)-").matcher(src);
            if (matcher.find()) {
                keyword = matcher.group(1);
                break;
            }
            
            // community/123W0QQ
            matcher = Pattern.compile("community/([^/\\?]+?)w0qq").matcher(src);
            if (matcher.find()) {
                keyword = matcher.group(1);
                break;
            }
            
            // W0QQkwZ123QQrdZ1
            matcher = Pattern.compile("qqkwz([^/\\?]+?)(qq|\\?|$)").matcher(src);
            if (matcher.find()) {
                keyword = matcher.group(1);
                break;
            }
            
        } while (false);
        
        if (keyword == null || "".equals(keyword))
            return null;

        // 形如u5047%u65E5%u5609%u56ED的状况(unicode)
        if (keyword.startsWith("%u"))
            return parseUniCode(keyword);
        
        if (encoding == null)
            encoding = defaultEncoding;

        if (!checkStrEncoding(keyword, encoding))
            keyword = tryEncoding(keyword);
        else
            keyword = URLDecoder.decode(keyword, encoding);

        return keyword;
    }

    //尝试以常见编码解码
    private static String tryEncoding(String keyword) throws UnsupportedEncodingException
    {
        String[] encodings = { "utf8", "gbk", "big5", "JIS" };
        for (int i = 0; i < encodings.length; i++)
        {
            String  ret = URLDecoder.decode(keyword, encodings[i]);
            // 解码成功就退出了
            if (checkStrEncoding(ret, encodings[i]))
               return ret;
        }
        
        //都不行就返回空，无法解码
        return null;
    }

    // 判断字符串的编码准确性
    private static boolean checkStrEncoding(String src, String encoding)
    {
        if (src == null || "".equals(src))
            return false;

        try
        {
            //重复一次编码解码操作，正常情况下相抵消
            String decodeRet = URLDecoder.decode(src, encoding);
            String decodeNext = URLDecoder.decode(URLEncoder.encode(decodeRet, encoding), encoding);
            return decodeRet.equals(decodeNext);
        }
        catch (UnsupportedEncodingException e)
        {
            return false;
        }
    }
}
