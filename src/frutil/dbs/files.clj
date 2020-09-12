(ns frutil.dbs.files
  (:require
   [clojure.java.io :as io]))


(defn delete [path]
  (let [file (io/as-file path)]
    (when (-> file .exists)
      (when (-> file .isDirectory)
        (doseq [file (-> file .listFiles)]
          (delete file)))
      (io/delete-file file))))


(defn file-from [file-path dirs]
  (if (or (empty? dirs)
          (.contains file-path ".."))
    nil
    (let [dir (first dirs)
          file (java.io.File. (str dir "/" file-path))]
      (if (.exists file)
        file
        (file-from file-path (rest dirs))))))

(comment
  (file-from "deps.edn" ["/home/witek/inbox" "/p/frutil/dbs"]))
