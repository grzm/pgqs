(ns com.grzm.pgqs.build
  (:require
   [clojure.data.json :as json]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]))

(defn fetch-git-repo-root []
  (str/trim (:out (apply shell/sh ["git" "rev-parse" "--show-toplevel"]))))

(defn read-edn [readerable]
  (edn/read-string (slurp readerable)))

(defn pprint-json-str [json]
  (with-out-str (json/pprint json)))

(defn format-action
  [{:keys [documentation request response] :as action}]
  (cond->
      [\newline
       (format "### %s" (:name action))
       \newline
       documentation
       \newline
       (format "#### %s" "Request")
       "```json"
       (pprint-json-str request)
       "```"]
    response (concat [\newline
                      (format "#### %s" "Response")
                      "```json"
                      (pprint-json-str response)
                      "```"])))

(defn build-docs []
  (let [doc-data (read-edn (io/file "etc/pgqs-ops.edn"))
        repo-root (fetch-git-repo-root)
        prelude ["# pgqs Actions"
                 \newline
                 "## Usage"
                 \newline
                 "```sql"
                 "SELECT pgqs.invoke(:action, :request)"
                 "```"]
        content (->> doc-data
                     (vals)
                     (sort-by :name)
                     (mapcat format-action)
                     (into prelude)
                     (str/join \newline))]
    (spit (io/file repo-root "actions.markdown") content)))

(comment
  (build-docs)
  :edn)
