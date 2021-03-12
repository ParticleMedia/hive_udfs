package com.newsbreak.data.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ComputeLabelUDF
 * @author Xuetao
 */

@Description(name = "ComputeLabelUDF", value = "ComputeLabelUDF(checked, clicked, shared, pv_time, cv_time, vv_time, thumb_up, thumb_down)",
        extended = "")
public class ComputeLabelUDF extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 8) {
            throw new UDFArgumentLengthException(
                    "ComputeLabelUDF(checked, clicked, autoplay, shared, liked, thumbed_up, thumbed_down, pv_time, cv_time, vv_time)");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        // tinyint
        for (int i = 0; i < 7; i++) {
converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        }

        // int features
        for (int i = 7; i < 10; i++) {
converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }

        converters[10] = ObjectInspectorConverters.getConverter(arguments[10],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[11] = ObjectInspectorConverters.getConverter(arguments[11],
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException, JSONException {
        assert (arguments.length == 10);

        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        short checked = (Short) converters[0].convert(arguments[0].get());
        short clicked = (Short) converters[1].convert(arguments[1].get());
        short autoplay = (Short) converters[2].convert(arguments[2].get());
        short shared = (Short) converters[3].convert(arguments[3].get());
        short liked = (Short) converters[4].convert(arguments[4].get());
        short thumbed_up = (Short) converters[5].convert(arguments[5].get());
        short thumbed_down = (Short) converters[6].convert(arguments[6].get());
        int pv_time = (Integer) converters[7].convert(arguments[7].get());
        int cv_time = (Integer) converters[8].convert(arguments[8].get());
        int vv_time = (Integer) converters[9].convert(arguments[9].get());
        String ctype = (String) converters[10].convert(arguments[10].get());
        float progress = (Float) converters[11].convert(arguments[11].get());

        return AssignLabel(ctype, checked, clicked, autoplay, shared, liked, thumbed_up, thumbed_down, pv_time, cv_time, vv_time, progress);
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 10);
        return "ComputeLabelUDF()";
    }

    private Boolean AssignLabel(String ctype, short checked, short clicked, short autoplay, short shared, short liked, short thumbed_up, short thumbed_down, int pv_time, int cv_time, int vv_time, float progress)
    {
        if (shared == 1 || thumbed_up == 1 || liked == 1)
        {
            return true;
        }

        if (clicked == 1) {
            if (ctype.equals("video_web")) {
                return vv_time > 10000 || progress > 0.2;
            }
            else {
                return pv_time > 1999;
            }
        }

        if (autoplay == 1) {
            return vv_time >= 20000;
        }

        return false;
    }
}