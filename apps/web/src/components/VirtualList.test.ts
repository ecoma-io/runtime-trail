import { describe, expect, it } from "vitest";
import { nextTick } from "vue";
import { mount } from "@vue/test-utils";

import VirtualList from "./VirtualList.vue";

const ITEM_HEIGHT = 32;
const COUNT = 100;
const items = Array.from({ length: COUNT }, (_, i) => `item-${i}`);

/** Fake a real viewport on the scroll container, then re-sync the window. */
function stubViewport(
  el: HTMLElement,
  clientHeight: number,
  scrollTop = 0,
): void {
  Object.defineProperty(el, "clientHeight", {
    configurable: true,
    value: clientHeight,
  });
  Object.defineProperty(el, "scrollTop", {
    configurable: true,
    value: scrollTop,
    writable: true,
  });
  el.dispatchEvent(new Event("scroll"));
}

describe("VirtualList (app-local loom API stopgap)", () => {
  it("renders only the viewport window plus overscan, with roving tab stops", async () => {
    const wrapper = mount(VirtualList, {
      props: { items, itemHeight: ITEM_HEIGHT },
      slots: { default: '<span class="row-name">{{ item }}</span>' },
    });
    stubViewport(
      wrapper.find("[data-loom-virtual-list]").element as HTMLElement,
      400,
    );
    // The scroll handler re-measures synchronously; the re-render is queued.
    await nextTick();

    // 13 visible rows (400/32, rounded up) + 8 below the window; the
    // overscan above only applies once scrolled away from the top.
    const rows = wrapper.findAll("[data-virtual-index]");
    expect(rows).toHaveLength(21);
    expect(rows[0]!.attributes("aria-setsize")).toBe(String(COUNT));
    expect(rows[0]!.attributes("aria-posinset")).toBe("1");
    expect(rows[20]!.attributes("aria-posinset")).toBe("21");
    // Exactly one tab stop: the first row while nothing is active.
    expect(rows[0]!.attributes("tabindex")).toBe("0");
    expect(rows[1]!.attributes("tabindex")).toBe("-1");
  });

  it("moves the active row on ArrowDown, roving the tab stop with it", async () => {
    const wrapper = mount(VirtualList, {
      props: { items, itemHeight: ITEM_HEIGHT },
      slots: { default: '<span class="row-name">{{ item }}</span>' },
    });
    stubViewport(
      wrapper.find("[data-loom-virtual-list]").element as HTMLElement,
      400,
    );

    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: "ArrowDown" });
    expect(wrapper.emitted("update:activeIndex")).toEqual([[1]]);
    // The consumer's v-model answers the emit (jsdom sets no focus itself);
    // Playwright asserts the real focus move.
    await wrapper.setProps({ activeIndex: 1 });
    await nextTick();
    expect(wrapper.get('[data-virtual-index="1"]').attributes("tabindex")).toBe(
      "0",
    );
    expect(wrapper.get('[data-virtual-index="0"]').attributes("tabindex")).toBe(
      "-1",
    );
  });

  it("activates the active row with Enter and Space", async () => {
    const wrapper = mount(VirtualList, {
      props: { items, itemHeight: ITEM_HEIGHT },
      slots: { default: '<span class="row-name">{{ item }}</span>' },
    });
    stubViewport(
      wrapper.find("[data-loom-virtual-list]").element as HTMLElement,
      400,
    );

    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: "Enter" });
    expect(wrapper.emitted("activate")).toEqual([[0]]);
    await wrapper
      .get('[data-virtual-index="0"]')
      .trigger("keydown", { key: " " });
    expect(wrapper.emitted("activate")).toEqual([[0], [0]]);
  });

  it("leaves keys typed inside a nested control to that control", async () => {
    const wrapper = mount(VirtualList, {
      props: { items, itemHeight: ITEM_HEIGHT },
      slots: {
        default: '<button type="button" class="inner">{{ item }}</button>',
      },
    });
    stubViewport(
      wrapper.find("[data-loom-virtual-list]").element as HTMLElement,
      400,
    );

    await wrapper.get("button.inner").trigger("keydown", { key: "ArrowDown" });
    expect(wrapper.emitted("update:activeIndex")).toBeUndefined();
  });

  it("clamps the active index when the list shrinks past it", async () => {
    const wrapper = mount(VirtualList, {
      props: { items: [1, 2, 3], itemHeight: ITEM_HEIGHT, activeIndex: 2 },
      slots: { default: '<span class="row-name">{{ item }}</span>' },
    });

    await wrapper.setProps({ items: [] });
    expect(wrapper.emitted("update:activeIndex")).toEqual([[-1]]);

    await wrapper.setProps({ items: [1, 2, 3], activeIndex: 2 });
    await wrapper.setProps({ items: [1] });
    expect(wrapper.emitted("update:activeIndex")).toEqual([[-1], [0]]);
  });
});
