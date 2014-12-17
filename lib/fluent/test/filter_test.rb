#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module Fluent
  module Test
    class FilterTestDriver < TestDriver
      def initialize(klass, tag = 'filter.test', &block)
        super(klass, &block)
        @tag = tag
        @events = {}
        @result = nil
      end

      def filter(tag, time, record)
        @instance.filter(tag, time, record)
      end

      def filter_stream(tag, es)
        @instance.filter_stream(tag, es)
      end

      def emit(record, time = Engine.now, tag = @tag)
        @events[tag] ||= MultiEventStream.new
        @events[tag].add(time, record)
      end

      def emits(events)
        events.each { |record, time, tag|
          emit(record, time.nil? ? Engine.now : time, tag.nil? ? @tag : tag)
        }
      end

      # Almost filters don't use a thread so default is 0. It reduces test time.
      def run(num_waits = 0, &block)
        @result = MultiEventStream.new
        super(num_waits) {
          block.call if block

          @events.each { |tag, es|
            processed = filter_stream(tag, es)
            processed.each { |time, record|
              @result.add(time, record)
            }
          }
        }
        @result
      end
    end
  end
end
