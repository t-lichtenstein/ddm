import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class Args {
    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Parameter(names = {"-c", "--cores"}, description = "Number of CPU-cores")
    private Integer cores = 4;

    @Parameter(names = {"-p", "--path"}, description = "Input dataset path")
    private String datasetPath = "./TPCH";

    public String getDatasetPath() {
        return datasetPath;
    }

    public Integer getCores() {
        return cores;
    }
}