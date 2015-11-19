package hello.security;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

public class AesCrypto
{
  public static Key skey = makeAESKey("ascdefghijvlmeop");

  public static String getEncode(String text) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException
  {
    text = padding16(text);

    byte[] enc = aesEncode(text.getBytes(), skey);

    return byte2hex(enc);
  }

  public static String getDecode(String text) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException
  {
    byte[] enc = hexToByteArray(text);

    byte[] dec = aesDecode(enc, skey);

    return new String(dec).trim();
  }

  public static String byte2hex(byte[] b)
  {
    String hs = "";

    String stmp = "";

    for (int n = 0; n < b.length; n++)
    {
      stmp = Integer.toHexString(b[n] & 0xFF);

      if (stmp.length() == 1) hs = hs + "0" + stmp;
      else {
        hs = hs + stmp;
      }
      if (n >= b.length - 1) continue; hs = hs;
    }

    return hs;
  }

  public static byte[] hexToByteArray(String hex)
  {
    if ((hex == null) || (hex.length() == 0))
    {
      return null;
    }

    byte[] ba = new byte[hex.length() / 2];

    for (int i = 0; i < ba.length; i++)
    {
      ba[i] = (byte)Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }

    return ba;
  }

  public static Key makeAESKey(String sKey)
  {
    byte[] key = sKey.getBytes();

    return new SecretKeySpec(key, "AES");
  }

  public static byte[] aesEncode(byte[] src, Key skey) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
  {

      Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");

      cipher.init(1, skey);

      return cipher.doFinal(src);

  }

  public static byte[] aesDecode(byte[] src, Key skey) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
  {
      Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");

      cipher.init(2, skey);

      return cipher.doFinal(src);
  }

  public static String padding16(String args)
  {
    int nCount = 16 - args.length() % 16;

    for (int i = 0; i < nCount; i++)
    {
      args = args + ' ';
    }

    return args;
  }
}