defmodule Coflux.Topics.Projects do
  alias Coflux.Projects

  use Topical.Topic, route: ["projects"]

  @server Coflux.ProjectsServer

  def connect(params, context) do
    namespace = Map.get(context, :namespace)
    {:ok, Map.put(params, :namespace, namespace)}
  end

  def init(params) do
    namespace = Map.get(params, :namespace)
    {ref, projects} = Projects.subscribe(@server, self(), namespace)
    {:ok, Topic.new(projects, %{ref: ref})}
  end

  def handle_info({:project, _ref, project_id, project}, topic) do
    topic = Topic.set(topic, [project_id], project)
    {:ok, topic}
  end
end
