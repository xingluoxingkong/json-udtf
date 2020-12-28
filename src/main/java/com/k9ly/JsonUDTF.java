package com.k9ly;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();

        if (inputFields.size() < 2) {
            throw new UDFArgumentException("参数不足，至少两个。");
        }

        ArrayList<String> fieldNames = new ArrayList(inputFields.size());
        ArrayList<ObjectInspector> fieldOIs = new ArrayList(inputFields.size());

        for (int i = 1; i < inputFields.size(); i++) {
            ObjectInspector inspector = inputFields.get(i).getFieldObjectInspector();
            if (inspector.getCategory() != ObjectInspector.Category.PRIMITIVE || !inspector.getTypeName().equals("string")) {
                throw new UDFArgumentException("参数类型错误，必须是string类型。");
            }
            fieldNames.add("c" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }


        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String jsonStr = objects[0].toString();
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        ArrayList<String> resList = new ArrayList<>();

        for (int i = 1; i < objects.length; i++) {
            resList.add(parse(jsonObject, objects[i].toString()));
        }

        forward(resList);
    }

    private String parse(Object jsonObject, String path) {
        try {
            if (path == null || "".equals(path)) {
                return jsonObject.toString();
            }
            // 找到第一个点
            int i1 = path.indexOf('.');
            if (i1 < 0 && !path.contains("[")) {
                // 没有点则证明是最后一个，直接取元素
                return ((Map) jsonObject).get(path).toString();
            }
            String parent = path;
            String children = "";
            if (i1 > 0) {
                parent = path.substring(0, i1);
                children = path.substring(i1 + 1);
            }

            // 判断是否是数组，[i]取下标，如果是[*]则循环取全部
            if (parent.contains("[") && parent.contains("]")) {
                String parentName = parent.substring(0, parent.indexOf("["));
                List jsonList = (List) ((Map) jsonObject).get(parentName);
                String index = parent.substring(parent.indexOf("[") + 1, parent.indexOf("]")).trim();
                if ("*".equals(index)) {
                    ArrayList<String> resList = new ArrayList<>();
                    for (Object obj : jsonList) {
                        resList.add(parse(obj, children));
                    }
                    return resList.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("");
                } else if (index.startsWith("@")) {
                    Object res = null;
                    String name = index.substring(1, index.indexOf("=")).trim();
                    String value = index.substring(index.indexOf("=") + 1).trim();
                    for (Object obj : jsonList) {
                        if(((Map) obj).getOrDefault(name, "").toString().equals(value))  {
                            res = obj;
                            break;
                        }
                    }
                    return parse(res, children);
                }

                else {
                    return parse(jsonList.get(Integer.parseInt(index)), children);
                }
            }

            return parse(((Map) jsonObject).get(parent), children);
        } catch (Exception e) {
            return "";
        }

    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) {
        String jsonStr = "{\"dataList\":[{\"flag\":0,\"name\":\"游戏\",\"value\":\"0.7\"},{\"flag\":0,\"name\":\"电影\",\"value\":\"0.6\"}],\"source\":\"2\",\"type\":\"兴趣爱好\"}";
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        JsonUDTF jsonUDTF = new JsonUDTF();
        String s = jsonUDTF.parse(jsonObject, "dataList[@name=游戏]");
        System.out.println(s);
    }
}

