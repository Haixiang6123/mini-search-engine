package edu.uci.ics.cs221.positional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor implements Compressor {

    @Override
    public byte[] encode(List<Integer> integers) {
        if(integers == null || integers.size() == 0)
            return new byte[0];

        // Process gaps
        int[] gaps = new int[integers.size()];
        gaps[0] = integers.get(0);
        for(int i = integers.size() - 1; i >= 1; i-- ) {
            gaps[i] = integers.get(i) - integers.get(i-1);
//            System.out.println(gaps[i] + " = "+ integers.get(i) + " - " + integers.get(i-1));
        }
        System.out.println("Gaps:" + Arrays.toString(gaps));

        //Process bytes:
        List<Byte> codes = new ArrayList<>();
        for( int i :gaps){
            List<Byte> encodeParts = new ArrayList<>();
            int num = i;
            int slice = 0;
            while(num > 0){
                // Get lowest 7 bit & shift num by 7 bit
                slice = num & 0x7F;
                num >>= 7;

                System.out.println(slice);

                // Put 7 bit slice in encode array
                if (encodeParts.size() > 0)     // Put lowest bit in the byte.
                    slice += (1 << 7);
                encodeParts.add((byte)slice);     //add(0, x) does not work?
                slice = 0;
            }

            System.out.println(slice);
            if (encodeParts.size() == 0)   // non added : num was 0
                encodeParts.add((byte)slice);

//            System.out.println(encodeParts);  // before reverse

            Collections.reverse(encodeParts);
            System.out.println("Encoded bytes:" + encodeParts);
            codes.addAll(encodeParts);
        }

        byte[] result = new byte[codes.size()];
        for(int i = 0 ; i < codes.size(); i++){
            result[i] = codes.get(i);
        }

        return result;
    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {
        if(length < 0 || start + length > bytes.length)
            return null;    //todo null or others?

        // Read from variable-length bytes
        List<Integer> result = new ArrayList<>();
        int num = 0;
        for(int i = start; i < start + length; i++){
            int newInt = bytes[i] & 0xFF;  // Get unsigned number
            if(newInt > 127)  // 1 on higest bit todo get bit from byte directly
                num = (num << 7) + (newInt & 0x7F); // delete 1 & concatenate last 7 bit
            else {           // 0 on highest bit
                num = (num << 7) + newInt;     // num[] + newInt[1:]
                System.out.println("decoded num: " + num);
                result.add(num);
                num = 0;
            }
        }

        System.out.println(" Decodes gaps: " + result);
        // Fill the gap
        for(int i = 1 ; i < result.size(); i++ ){
            result.set(i, result.get(i) + result.get(i -1));
        }

        return result;
    }
}
