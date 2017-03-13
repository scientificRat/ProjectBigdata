package javautils;

import java.io.*;

/**
 * Created by sky on 2017/3/13.
 */
public class ChangeData {

    private String constructNewLine(String[] arr) {
        StringBuilder stringBuilder = new StringBuilder();
        int i;
        for (i = 0; i < arr.length - 1; i++) {
            stringBuilder.append(arr[i]).append(" ");
        }
        return stringBuilder.append(arr[i]).toString();
    }

    public void changeData(long[] arr) {

        try (InputStreamReader in = new InputStreamReader(new FileInputStream("localData/user.txt"));
        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream("localData/new_user.log"))) {
            BufferedReader bufferedReader = new BufferedReader(in);
            BufferedWriter bufferedWriter = new BufferedWriter(out);
            int i;
            for (i = 0; i < arr.length; i++) {
                String line = bufferedReader.readLine();
                String[] in_arr = line.split(" ");
                in_arr[0] = String.valueOf(arr[i]);
                String newLine = constructNewLine(in_arr);
                bufferedWriter.write(newLine);
                bufferedWriter.newLine();
                bufferedWriter.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
